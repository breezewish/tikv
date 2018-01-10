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

use super::*;

use std::error;
use storage;

quick_error!{
    #[derive(Debug)]
    pub enum Error {
        SchedulerBusy(task_detail: String) {
            description("worker scheduler is busy")
            display("worker scheduler is busy, task = {}", task_detail)
        }
        SchedulerStopped(task_detail: String) {
            description("worker scheduler is stopped")
            display("worker scheduler is stopped, task = {}", task_detail)
        }
        PoolBusy(task_detail: String) {
            description("worker thread pool is busy")
            display("worker thread pool is busy, task = {}", task_detail)
        }
        Storage(err: storage::Error) {
            from()
            cause(err)
            description(err.description())
            display("storage error, err = {:?}", err)
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
    }
}
