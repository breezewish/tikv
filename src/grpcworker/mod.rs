// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod task;

use std::{io, result, sync};

use util::threadpool::{self, ThreadPool, ThreadPoolBuilder};
use util::worker::{Runnable, ScheduleError, Scheduler, Worker};
use storage::Engine;
use server::Config;

pub use self::task::{Callback, Error, Priority, Result, Step, Value};

struct RunnerEnvironment {
    pool_read_critical: ThreadPool<WorkerThreadContext>,
    pool_read_high: ThreadPool<WorkerThreadContext>,
    pool_read_normal: ThreadPool<WorkerThreadContext>,
    pool_read_low: ThreadPool<WorkerThreadContext>,
    max_read_tasks: usize,
}

pub struct WorkerThreadContext {
    engine: Box<Engine>,
}

impl threadpool::Context for WorkerThreadContext {}

struct WorkerThreadContextFactory {
    engine: Box<Engine>,
}

impl threadpool::ContextFactory<WorkerThreadContext> for WorkerThreadContextFactory {
    fn create(&self) -> WorkerThreadContext {
        WorkerThreadContext {
            engine: self.engine.clone(),
        }
    }
}

impl RunnerEnvironment {
    fn new(
        read_critical_concurrency: usize,
        read_high_concurrency: usize,
        read_normal_concurrency: usize,
        read_low_concurrency: usize,
        max_read_tasks: usize,
        stack_size: usize,
        engine: Box<Engine>,
    ) -> RunnerEnvironment {
        RunnerEnvironment {
            max_read_tasks,
            pool_read_critical: ThreadPoolBuilder::new(
                thd_name!("grpc-request-pool-read-critical"),
                WorkerThreadContextFactory {
                    engine: engine.clone(),
                },
            ).thread_count(read_critical_concurrency)
                .stack_size(stack_size)
                .build(),
            pool_read_high: ThreadPoolBuilder::new(
                thd_name!("grpc-request-pool-read-high"),
                WorkerThreadContextFactory {
                    engine: engine.clone(),
                },
            ).thread_count(read_high_concurrency)
                .stack_size(stack_size)
                .build(),
            pool_read_normal: ThreadPoolBuilder::new(
                thd_name!("grpc-request-pool-read-normal"),
                WorkerThreadContextFactory {
                    engine: engine.clone(),
                },
            ).thread_count(read_normal_concurrency)
                .stack_size(stack_size)
                .build(),
            pool_read_low: ThreadPoolBuilder::new(
                thd_name!("grpc-request-pool-read-low"),
                WorkerThreadContextFactory {
                    engine: engine.clone(),
                },
            ).thread_count(read_low_concurrency)
                .stack_size(stack_size)
                .build(),
        }
    }

    fn shutdown(&mut self) {
        if let Err(e) = self.pool_read_critical.stop() {
            warn!("Stop pool_read_critical failed with {:?}", e);
        }
        if let Err(e) = self.pool_read_high.stop() {
            warn!("Stop pool_read_high failed with {:?}", e);
        }
        if let Err(e) = self.pool_read_normal.stop() {
            warn!("Stop pool_read_normal failed with {:?}", e);
        }
        if let Err(e) = self.pool_read_low.stop() {
            warn!("Stop pool_read_low failed with {:?}", e);
        }
    }

    /// Check whether tasks in any read pool exceeds the limit.
    fn is_read_busy(&self) -> bool {
        self.pool_read_critical.get_task_count() >= self.max_read_tasks ||
            self.pool_read_high.get_task_count() >= self.max_read_tasks ||
            self.pool_read_normal.get_task_count() >= self.max_read_tasks ||
            self.pool_read_low.get_task_count() >= self.max_read_tasks
    }

    /// Get a mutable reference for the thread pool by the specified priority flag.
    fn get_pool_by_priority(&self, priority: Priority) -> &ThreadPool<WorkerThreadContext> {
        match priority {
            Priority::ReadCritical => &self.pool_read_critical,
            Priority::ReadHigh => &self.pool_read_high,
            Priority::ReadNormal => &self.pool_read_normal,
            Priority::ReadLow => &self.pool_read_low,
        }
    }
}

#[inline]
fn async_execute_task(
    runner_env: &RunnerEnvironment,
    scheduler: &Scheduler<task::Task>,
    t: task::Task,
) {
    if runner_env.is_read_busy() {
        (t.callback)(Err(Error::Busy));
        return;
    }
    match scheduler.schedule(t) {
        Err(ScheduleError::Full(t)) => (t.callback)(Err(Error::Busy)),
        Err(ScheduleError::Stopped(_)) => panic!("worker scheduler is stopped"), // should we panic?
        Ok(_) => (),
    }
}

#[inline]
fn async_execute_step(
    runner_env: &RunnerEnvironment,
    scheduler: &Scheduler<task::Task>,
    step: Box<Step>,
    priority: Priority,
    callback: Callback,
) {
    let t = task::Task {
        callback,
        step: Some(step),
        priority,
    };
    async_execute_task(runner_env, scheduler, t);
}

struct Runner {
    // need locks here because shutdown is mutable
    runner_env: sync::Arc<sync::RwLock<RunnerEnvironment>>,
    scheduler: Scheduler<task::Task>,
}

impl Runnable<task::Task> for Runner {
    fn run(&mut self, mut t: task::Task) {
        let scheduler = self.scheduler.clone();
        let runner_env = self.runner_env.clone();

        let env_instance = self.runner_env.read().unwrap();
        let pool = env_instance.get_pool_by_priority(t.priority);

        pool.execute(move |context: &mut WorkerThreadContext| {
            let step = t.step.take().unwrap();
            step.async_work(context, box move |result: task::StepResult| match result {
                task::StepResult::Continue(new_step) => {
                    t.step = Some(new_step);
                    async_execute_task(&runner_env.read().unwrap(), &scheduler, t);
                }
                task::StepResult::Finish(result) => {
                    (t.callback)(result);
                }
            });
        });
    }
}

pub struct GrpcRequestWorker {
    runner_env: sync::Arc<sync::RwLock<RunnerEnvironment>>,

    // `worker` is protected via a mutex to prevent concurrent mutable access
    // (i.e. `worker.start()` & `worker.stop()`)
    worker: sync::Arc<sync::Mutex<Worker<task::Task>>>,

    // `scheduler` is extracted from the `worker` so that we don't need to lock the worker
    // (to get the `scheduler`) when pushing items into the queue.
    scheduler: Scheduler<task::Task>,
}

impl GrpcRequestWorker {
    pub fn new(config: &Config, engine: Box<Engine>) -> GrpcRequestWorker {
        let runner_env = sync::Arc::new(sync::RwLock::new(RunnerEnvironment::new(
            config.grpc_worker_read_critical_concurrency,
            config.grpc_worker_read_high_concurrency,
            config.grpc_worker_read_normal_concurrency,
            config.grpc_worker_read_low_concurrency,
            config.grpc_worker_max_read_tasks,
            config.grpc_worker_stack_size.0 as usize,
            engine,
        )));
        let worker = Worker::new("grpc-request-worker");
        let scheduler = worker.scheduler();
        GrpcRequestWorker {
            runner_env,
            worker: sync::Arc::new(sync::Mutex::new(worker)),
            scheduler,
        }
    }

    /// Execute a task on the specified thread pool and get the result when it is finished.
    ///
    /// The caller should ensure the matching of the step and its priority, for example, for
    /// tasks about reading, the priority should be ReadXxx and the behavior is undefined if a
    /// WriteXxx priority is specified instead.
    pub fn async_execute(&self, begin_step: Box<Step>, priority: Priority, callback: Callback) {
        async_execute_step(
            &self.runner_env.read().unwrap(),
            &self.scheduler,
            begin_step,
            priority,
            callback,
        );
    }

    pub fn start(&mut self) -> result::Result<(), io::Error> {
        let mut worker = self.worker.lock().unwrap();
        let runner = Runner {
            runner_env: self.runner_env.clone(),
            scheduler: self.scheduler.clone(),
        };
        worker.start(runner)
    }

    pub fn shutdown(&mut self) {
        // TODO: shutdown runner_env inside worker to eliminate locks in async_execute.
        let mut worker = self.worker.lock().unwrap();
        if let Err(e) = worker.stop().unwrap().join() {
            error!("failed to stop GrpcWorker: {:?}", e);
        }
        self.runner_env.write().unwrap().shutdown();
    }
}

impl Clone for GrpcRequestWorker {
    fn clone(&self) -> GrpcRequestWorker {
        GrpcRequestWorker {
            runner_env: self.runner_env.clone(),
            worker: self.worker.clone(),
            scheduler: self.scheduler.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use util::worker::Worker;
    use std::sync::mpsc::{channel, Sender};
    use storage;
    use kvproto::kvrpcpb;
    use super::*;

    fn expect_ok(done: Sender<i32>, id: i32) -> Callback {
        Box::new(move |x: Result| {
            assert!(x.is_ok());
            done.send(id).unwrap();
        })
    }

    fn expect_get_none(done: Sender<i32>, id: i32) -> Callback {
        Box::new(move |x: Result| {
            assert!(x.is_ok());
            assert_eq!(x.unwrap(), task::Value::StorageValue(None));
            done.send(id).unwrap();
        })
    }

    fn expect_get_val(done: Sender<i32>, v: Vec<u8>, id: i32) -> Callback {
        Box::new(move |x: Result| {
            assert!(x.is_ok());
            assert_eq!(x.unwrap(), task::Value::StorageValue(Some(v)));
            done.send(id).unwrap();
        })
    }

    #[test]
    fn test_scheduler_run() {
        let storage_config = storage::Config::default();
        let mut storage = storage::Storage::new(&storage_config).unwrap();

        let (tx, rx) = channel();

        let worker_config = Config::default();
        let mut grpc_worker = GrpcRequestWorker::new(&worker_config, storage.get_engine());
        grpc_worker.start().unwrap();

        grpc_worker.async_execute(
            box task::read::KvGetStep {
                req_context: kvrpcpb::Context::new(),
                key: b"x".to_vec(),
                start_ts: 100,
            },
            task::Priority::ReadCritical,
            expect_get_none(tx.clone(), 0),
        );
        assert_eq!(rx.recv().unwrap(), 0);

        grpc_worker.shutdown();
    }
}
