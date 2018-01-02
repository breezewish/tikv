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

extern crate core;

use futures::sync::oneshot;
use std::{any, error};
use std::fmt;

pub struct Task {
    pub future_sender: oneshot::Sender<super::job::JobResult>,
    pub work_result: Option<super::job::JobResult>,
    pub job: Box<super::job::Job + Send>,
    pub priority: Priority,
    pub current_state: State,
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Task {:?}", self.current_state)
    }
}

impl Task {}

#[derive(Debug, Copy, Clone)]
pub enum Priority {
    ReadNormal,
    ReadLow,
    ReadHigh,
    ReadCritical,
}

#[derive(Debug, Copy, Clone)]
pub enum State {
    BeforePre,
    BeforeWork,
    BeforePost,
    Done,
}
