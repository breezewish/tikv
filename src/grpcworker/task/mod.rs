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

pub mod kvget;
pub mod cop;
mod util;

use std::{boxed, fmt, result};
use storage;
use kvproto::coprocessor as coppb;

use super::*;

#[derive(Debug, Copy, Clone)]
pub enum Priority {
    ReadNormal,
    ReadLow,
    ReadHigh,
    ReadCritical,
}

#[derive(Debug, PartialEq)]
pub enum Value {
    Storage(Option<storage::Value>),
    Coprocessor(coppb::Response),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::Storage(_) => write!(f, "Storage"),
            Value::Coprocessor(ref res) => write!(f, "Coprocessor {:?}", res),
        }
    }
}

pub type Result = result::Result<Value, Error>;
pub type Callback = Box<boxed::FnBox(Result) + Send>;

/// Task holds everything about a particular functionality. A task may consist of many sub tasks
/// to be executed. Only current sub task is stored in the task.
pub struct Task {
    pub callback: Callback,
    pub subtask: Option<Box<SubTask>>,
    pub priority: Priority,
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Task priority = {:?}, subtask = ", self.priority)?;
        match self.subtask {
            None => write!(f, "None"),
            Some(ref subtask) => write!(f, "{}", subtask),
        }
    }
}

pub enum SubTaskResult {
    /// Indicate that there are more sub tasks to be executed to do current functionality.
    Continue(Box<SubTask>),
    /// Indicate that current functionality is done.
    Finish(Result),
}

pub type SubTaskCallback = Box<boxed::FnBox(SubTaskResult) + Send>;

/// Sub task is a smallest single unit to be executed in the thread pool.
/// A complete functionality may be assembled by multiple sub tasks.
pub trait SubTask: Send + fmt::Display {
    fn async_work(self: Box<Self>, context: &mut WorkerThreadContext, on_done: SubTaskCallback);
}
