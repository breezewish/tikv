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

pub mod read;

use futures::{future, Future};
use std::{any, boxed, error, result};

#[derive(Debug, Copy, Clone)]
pub enum JobResult {
    Foo,
    Bar,
}

#[derive(Debug, Copy, Clone)]
pub enum JobError {
    ScheduleError,
    Busy,
    Canceled,
}

pub type JobFuture<T> = Box<Future<Item = T, Error = JobError>>;

pub trait Job {
    fn async_pre(&mut self) -> JobFuture<()> {
        Box::new(future::ok(()))
    }

    fn async_post(&mut self) -> JobFuture<()> {
        Box::new(future::ok(()))
    }

    fn async_work(&mut self) -> JobFuture<JobResult>;
}
