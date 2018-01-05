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
mod util;

use std::{boxed, fmt, result};

#[derive(Debug, Copy, Clone)]
pub enum Priority {
    ReadNormal,
    ReadLow,
    ReadHigh,
    ReadCritical,
}

#[derive(Debug, Copy, Clone)]
pub enum Value {
    Foo,
    Bar,
}

#[derive(Debug, Copy, Clone)]
pub enum Error {
    ScheduleError,
    Busy,
    Canceled,
}

pub type Result = result::Result<Value, Error>;
pub type Callback = Box<boxed::FnBox(Result) + Send>;

/// Task holds everything about a particular functionality. A task may consist of many steps
/// to be executed, each of which is a job. Only the latest job is stored in the task.
pub struct Task {
    pub callback: Callback,
    pub step: Box<Step>,
    pub priority: Priority,
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Task priority = {:?}, step = {}",
            self.priority,
            self.step
        )
    }
}

pub enum StepResult {
    /// Indicate that there are more jobs to be executed to do current functionality.
    Continue(Box<Step>),
    /// Indicate that current functionality is done.
    Finish(Result),
}

pub type StepCallback = Box<boxed::FnBox(StepResult)>;

/// Step is a smallest single unit to be executed in the thread pool. A complete functionality may
/// be assembled by multiple steps.
pub trait Step: Send + fmt::Display {
    fn async_work(&self, on_done: StepCallback);
}
