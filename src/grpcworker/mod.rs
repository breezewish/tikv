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

mod task;
mod errors;

use std::{io, result, sync};

use util::threadpool::{self, ThreadPool, ThreadPoolBuilder};
use util::worker::{Runnable, ScheduleError, Scheduler, Worker};
use storage::Engine;
use server::Config;

pub use self::errors::Error;
pub use self::task::{Callback, Priority, Result, SubTask, Task, Value};
pub use self::task::read::*;

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
                thd_name!("grpcwkr-rc"),
                WorkerThreadContextFactory {
                    engine: engine.clone(),
                },
            ).thread_count(read_critical_concurrency)
                .stack_size(stack_size)
                .build(),
            pool_read_high: ThreadPoolBuilder::new(
                thd_name!("grpcwkr-rh"),
                WorkerThreadContextFactory {
                    engine: engine.clone(),
                },
            ).thread_count(read_high_concurrency)
                .stack_size(stack_size)
                .build(),
            pool_read_normal: ThreadPoolBuilder::new(
                thd_name!("grpcwkr-rn"),
                WorkerThreadContextFactory {
                    engine: engine.clone(),
                },
            ).thread_count(read_normal_concurrency)
                .stack_size(stack_size)
                .build(),
            pool_read_low: ThreadPoolBuilder::new(
                thd_name!("grpcwkr-rl"),
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

    /// Get a reference for the thread pool by the specified priority flag.
    #[inline]
    fn get_pool_by_priority(&self, priority: Priority) -> &ThreadPool<WorkerThreadContext> {
        match priority {
            Priority::ReadCritical => &self.pool_read_critical,
            Priority::ReadHigh => &self.pool_read_high,
            Priority::ReadNormal => &self.pool_read_normal,
            Priority::ReadLow => &self.pool_read_low,
        }
    }

    /// Check whether tasks in the pool exceeds the limit.
    #[inline]
    fn is_pool_busy(&self, pool: &ThreadPool<WorkerThreadContext>) -> bool {
        pool.get_task_count() >= self.max_read_tasks
    }
}

#[inline]
fn schedule_task(scheduler: &Scheduler<Task>, t: Task) {
    match scheduler.schedule(t) {
        Err(ScheduleError::Full(t)) => {
            let task_detail = format!("{}", t);
            (t.callback)(Err(Error::SchedulerBusy(task_detail)));
        }
        Err(ScheduleError::Stopped(t)) => {
            let task_detail = format!("{}", t);
            (t.callback)(Err(Error::SchedulerStopped(task_detail)));
        }
        Ok(_) => (),
    }
}

struct Runner {
    runner_env: RunnerEnvironment,
    scheduler: Scheduler<Task>,
}

impl Runnable<Task> for Runner {
    fn run(&mut self, mut t: Task) {
        let scheduler = self.scheduler.clone();
        let pool = self.runner_env.get_pool_by_priority(t.priority);
        if self.runner_env.is_pool_busy(pool) {
            let task_detail = format!("{}", t);
            (t.callback)(Err(Error::PoolBusy(task_detail)));
            return;
        }

        pool.execute(move |context: &mut WorkerThreadContext| {
            let subtask = t.subtask.take().unwrap();
            subtask.async_work(
                context,
                box move |result: task::SubTaskResult| match result {
                    task::SubTaskResult::Continue(new_subtask) => {
                        t.subtask = Some(new_subtask);
                        schedule_task(&scheduler, t);
                    }
                    task::SubTaskResult::Finish(result) => {
                        (t.callback)(result);
                    }
                },
            );
        });
    }

    fn shutdown(&mut self) {
        self.runner_env.shutdown();
    }
}

pub struct GrpcRequestWorker {
    read_critical_concurrency: usize,
    read_high_concurrency: usize,
    read_normal_concurrency: usize,
    read_low_concurrency: usize,
    max_read_tasks: usize,
    stack_size: usize,
    engine: Box<Engine>,

    /// `worker` is protected via a mutex to prevent concurrent mutable access
    /// (i.e. `worker.start()` & `worker.stop()`)
    worker: sync::Arc<sync::Mutex<Worker<Task>>>,

    /// `scheduler` is extracted from the `worker` so that we don't need to lock the worker
    /// (to get the `scheduler`) when pushing items into the queue.
    scheduler: Scheduler<Task>,
}

impl GrpcRequestWorker {
    pub fn new(config: &Config, engine: Box<Engine>) -> GrpcRequestWorker {
        let worker = Worker::new("grpcwkr-schd");
        let scheduler = worker.scheduler();
        GrpcRequestWorker {
            read_critical_concurrency: config.grpc_worker_read_critical_concurrency,
            read_high_concurrency: config.grpc_worker_read_high_concurrency,
            read_normal_concurrency: config.grpc_worker_read_normal_concurrency,
            read_low_concurrency: config.grpc_worker_read_low_concurrency,
            max_read_tasks: config.grpc_worker_max_read_tasks,
            stack_size: config.grpc_worker_stack_size.0 as usize,
            engine,
            worker: sync::Arc::new(sync::Mutex::new(worker)),
            scheduler,
        }
    }

    /// Execute a task on the specified thread pool and get the result when it is finished.
    ///
    /// The caller should ensure the matching of the sub task and its priority, for example, for
    /// tasks about reading, the priority should be ReadXxx and the behavior is undefined if a
    /// WriteXxx priority is specified instead.
    pub fn async_execute(
        &self,
        begin_subtask: Box<SubTask>,
        priority: Priority,
        callback: Callback,
    ) {
        let t = Task {
            callback,
            subtask: Some(begin_subtask),
            priority,
        };
        schedule_task(&self.scheduler, t);
    }

    pub fn start(&mut self) -> result::Result<(), io::Error> {
        let runner_env = RunnerEnvironment::new(
            self.read_critical_concurrency,
            self.read_high_concurrency,
            self.read_normal_concurrency,
            self.read_low_concurrency,
            self.max_read_tasks,
            self.stack_size,
            self.engine.clone(),
        );
        let mut worker = self.worker.lock().unwrap();
        let runner = Runner {
            runner_env,
            scheduler: self.scheduler.clone(),
        };
        worker.start(runner)
    }

    pub fn shutdown(&mut self) {
        let mut worker = self.worker.lock().unwrap();
        if let Err(e) = worker.stop().unwrap().join() {
            error!("failed to stop GrpcWorker: {:?}", e);
        }
    }
}

impl Clone for GrpcRequestWorker {
    fn clone(&self) -> GrpcRequestWorker {
        GrpcRequestWorker {
            read_critical_concurrency: self.read_critical_concurrency,
            read_high_concurrency: self.read_high_concurrency,
            read_normal_concurrency: self.read_normal_concurrency,
            read_low_concurrency: self.read_low_concurrency,
            max_read_tasks: self.max_read_tasks,
            stack_size: self.stack_size,
            engine: self.engine.clone(),
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
            assert_eq!(x.unwrap(), Value::StorageValue(None));
            done.send(id).unwrap();
        })
    }

    fn expect_get_val(done: Sender<i32>, v: Vec<u8>, id: i32) -> Callback {
        Box::new(move |x: Result| {
            assert!(x.is_ok());
            assert_eq!(x.unwrap(), Value::StorageValue(Some(v)));
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
            box KvGet {
                req_context: kvrpcpb::Context::new(),
                key: b"x".to_vec(),
                start_ts: 100,
            },
            Priority::ReadCritical,
            expect_get_none(tx.clone(), 0),
        );
        assert_eq!(rx.recv().unwrap(), 0);

        grpc_worker.shutdown();
    }
}
