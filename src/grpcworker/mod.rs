// Copyright 2017 PingCAP, Inc.
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

pub mod job;
pub mod task;

use futures::{future, Future};
use futures::sync::oneshot;
use std::{any, cell, error, io, rc, sync};

use util::threadpool::{Context, ContextFactory, ThreadPool, ThreadPoolBuilder};
use util::worker::{Builder as WorkerBuilder, Runnable, Scheduler, Worker};

struct RunnerEnvironment {
    pool_read_critical: ThreadPool<WorkerContext>,
    pool_read_high: ThreadPool<WorkerContext>,
    pool_read_normal: ThreadPool<WorkerContext>,
    pool_read_low: ThreadPool<WorkerContext>,
    max_read_tasks: usize,
}

impl RunnerEnvironment {
    fn new(concurrency: usize, max_read_tasks: usize, stack_size: usize) -> RunnerEnvironment {
        RunnerEnvironment {
            max_read_tasks,
            pool_read_critical: ThreadPoolBuilder::new(
                thd_name!("grpc-request-pool-read-critical"),
                WorkerContextFactory {},
            ).thread_count(concurrency)
                .stack_size(stack_size)
                .build(),
            pool_read_high: ThreadPoolBuilder::new(
                thd_name!("grpc-request-pool-read-high"),
                WorkerContextFactory {},
            ).thread_count(concurrency)
                .stack_size(stack_size)
                .build(),
            pool_read_normal: ThreadPoolBuilder::new(
                thd_name!("grpc-request-pool-read-normal"),
                WorkerContextFactory {},
            ).thread_count(concurrency)
                .stack_size(stack_size)
                .build(),
            pool_read_low: ThreadPoolBuilder::new(
                thd_name!("grpc-request-pool-read-low"),
                WorkerContextFactory {},
            ).thread_count(concurrency)
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
    fn get_pool_by_priority(&self, priority: task::Priority) -> &ThreadPool<WorkerContext> {
        match priority {
            task::Priority::ReadCritical => &self.pool_read_critical,
            task::Priority::ReadHigh => &self.pool_read_high,
            task::Priority::ReadNormal => &self.pool_read_normal,
            task::Priority::ReadLow => &self.pool_read_low,
        }
    }
}

struct Runner {
    runner_env: sync::Arc<sync::Mutex<RunnerEnvironment>>,
    scheduler: Scheduler<task::Task>,
}

impl Runnable<task::Task> for Runner {
    fn run(&mut self, mut t: task::Task) {
        let scheduler = self.scheduler.clone();
        info!(
            "GrpcRequestWorker::run, t.current_state = {:?}",
            t.current_state
        );
        let env = self.runner_env.lock().unwrap();
        let pool = env.get_pool_by_priority(t.priority);
        pool.execute(move |ctx: &mut WorkerContext| {
            let f = t.job.async_pre();
            f.and_then(move |_| {
                t.current_state = task::State::BeforeWork;
                scheduler.schedule(t);
                future::ok(())
            });
        });
        //        match t.current_state {
        //            task::State::BeforePre => {
        //                pool.execute(move |ctx: &mut WorkerContext| {
        //                    let mut j = t.job;
        //                    let f = j.async_pre();
        //                    f.and_then(|_| {
        //                        t.current_state = task::State::BeforeWork;
        //                        self.task_scheduler.schedule(t);
        //                        future::ok(())
        //                    });
        //                });
        //            }
        //            task::State::BeforeWork => {
        //                pool.execute(move |ctx: &mut WorkerContext| {
        //                    let mut j = t.job;
        //                    let f = j.async_work();
        //                    f.and_then(|r| {
        //                        t.work_result = Some(r);
        //                        t.current_state = task::State::BeforePost;
        //                        self.task_scheduler.schedule(t);
        //                        future::ok(())
        //                    });
        //                });
        //            }
        //            task::State::BeforePost => {
        //                pool.execute(move |ctx: &mut WorkerContext| {
        //                    let mut j = t.job;
        //                    let f = j.async_post();
        //                    f.and_then(|_| {
        //                        t.current_state = task::State::Done;
        //                        t.future_sender.send(t.work_result.unwrap());
        //                        future::ok(())
        //                    });
        //                });
        //            }
        //        }
    }
}

pub struct GrpcRequestWorker {
    runner_env: sync::Arc<sync::Mutex<RunnerEnvironment>>,
    worker: Worker<task::Task>,
}

impl GrpcRequestWorker {
    pub fn new(concurrency: usize, max_read_tasks: usize, stack_size: usize) -> GrpcRequestWorker {
        let runner_env = sync::Arc::new(sync::Mutex::new(RunnerEnvironment::new(
            concurrency,
            max_read_tasks,
            stack_size,
        )));
        let worker = Worker::new("grpc-request-worker");
        GrpcRequestWorker { runner_env, worker }
    }

    /// Add and execute a request job on the specified thread pool and get the result when it is
    /// finished. The future will be failed if tasks in thread pool exceeds the limit.
    ///
    /// The caller should ensure the matching of the task and its priority, for example, for
    /// tasks about reading, the priority should be ReadXxx and the behavior is undefined if a
    /// WriteXxx priority is specified instead.
    pub fn async_handle_job(
        &self,
        job: Box<job::Job + Send>,
        priority: task::Priority,
    ) -> job::JobFuture<job::JobResult> {
        if self.runner_env.lock().unwrap().is_read_busy() || self.worker.is_busy() {
            return Box::new(future::err(job::JobError::Busy));
        }
        info!("async_handle_job, priority = {}", priority);
        let (future_sender, future) = oneshot::channel::<job::JobResult>();
        let t = task::Task {
            future_sender,
            job,
            priority,
            current_state: task::State::BeforePre,
            work_result: None,
        };
        match self.worker.scheduler().schedule(t) {
            Err(e) => Box::new(future::err(job::JobError::Busy)),
            Ok(_) => Box::new(future.map_err(|_| job::JobError::Canceled)),
        }
    }

    pub fn start(&mut self) {
        let runner = Runner {
            runner_env: self.runner_env.clone(),
            scheduler: self.worker.scheduler(),
        };
        self.worker.start(runner);
    }

    pub fn shutdown(&mut self) {
        self.worker.stop();
        self.runner_env.lock().unwrap().shutdown();
    }
}

struct WorkerContext {}

impl Context for WorkerContext {}

struct WorkerContextFactory {}

impl ContextFactory<WorkerContext> for WorkerContextFactory {
    fn create(&self) -> WorkerContext {
        WorkerContext {}
    }
}

#[cfg(test)]
mod tests {
    use util::worker::Worker;
    use std::time::Duration;
    use std::sync;
    use futures::future;
    use super::*;

    #[test]
    fn test_scheduler_run() {
        let mut grpc_worker = GrpcRequestWorker::new(10, 10, 10240);
        grpc_worker.start();

        let (tx, rx) = sync::mpsc::channel();
        grpc_worker
            .async_handle_job(
                Box::new(job::read::JobCoprocessorDag {}),
                task::Priority::ReadCritical,
            )
            .then(|result| {
                match result {
                    Err(e) => {
                        info!("handle_job result: err = {:?}", e);
                    }
                    Ok(r) => {
                        info!("handle_job result: ok = {:?}", r);
                    }
                }
                tx.send(1);
                future::ok::<(), ()>(())
            });
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 1);
        grpc_worker.shutdown();
    }
}
