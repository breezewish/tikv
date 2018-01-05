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

pub mod task;

use std::sync;

use util::threadpool::{Context, ContextFactory, ThreadPool, ThreadPoolBuilder};
use util::worker::{Runnable, ScheduleError, Scheduler, Worker};

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

fn async_execute_step(
    runner_env: &RunnerEnvironment,
    scheduler: &Scheduler<task::Task>,
    step: Box<task::Step>,
    priority: task::Priority,
    callback: task::Callback,
) {
    if runner_env.is_read_busy() {
        callback(Err(task::Error::Busy));
        return;
    }
    info!(
        "async_execute_step, priority = {:?}, step = {}",
        priority,
        step
    );
    let t = task::Task {
        callback,
        step,
        priority,
    };
    match scheduler.schedule(t) {
        Err(ScheduleError::Full(t)) => (t.callback)(Err(task::Error::Busy)),
        Err(ScheduleError::Stopped(_)) => panic!("worker scheduler is stopped"), // should we panic?
        Ok(_) => (),
    }
}

struct Runner {
    // need mutex here because shutdown is mutable
    runner_env: sync::Arc<sync::Mutex<RunnerEnvironment>>,
    scheduler: Scheduler<task::Task>,
}

impl Runnable<task::Task> for Runner {
    fn run(&mut self, t: task::Task) {
        info!("GrpcRequestRunner::run");

        let scheduler = self.scheduler.clone();
        let runner_env = self.runner_env.clone();

        let env_instance = self.runner_env.lock().unwrap();
        let pool = env_instance.get_pool_by_priority(t.priority);

        pool.execute(move |ctx: &mut WorkerContext| {
            let priority = t.priority;
            let callback = t.callback;
            t.step.async_work(
                box (move |result: task::StepResult| match result {
                    task::StepResult::Continue(job) => {
                        async_execute_step(
                            &runner_env.lock().unwrap(),
                            &scheduler,
                            job,
                            priority,
                            callback,
                        );
                    }
                    task::StepResult::Finish(result) => {
                        callback(result);
                    }
                }),
            );
        });
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

    /// Execute a task on the specified thread pool and get the result when it is finished.
    ///
    /// The caller should ensure the matching of the step and its priority, for example, for
    /// tasks about reading, the priority should be ReadXxx and the behavior is undefined if a
    /// WriteXxx priority is specified instead.
    pub fn async_execute(
        &self,
        begin_step: Box<task::Step>,
        priority: task::Priority,
        callback: task::Callback,
    ) {
        async_execute_step(
            &self.runner_env.lock().unwrap(),
            &self.worker.scheduler(),
            begin_step,
            priority,
            callback,
        );
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
        grpc_worker.async_execute_step(
            Box::new(task::read::CoprocessorDagStep {}),
            task::Priority::ReadCritical,
            box (move |result| {
                match result {
                    Err(e) => {
                        println!("async_execute_step result: err = {:?}", e);
                    }
                    Ok(r) => {
                        println!("async_execute_step result: ok = {:?}", r);
                    }
                }
                tx.send(1);
            }),
        );
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), 1);
        grpc_worker.shutdown();
    }
}
