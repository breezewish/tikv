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

use storage;
use kvproto::kvrpcpb;
use std::fmt;

use super::{Error, Step, StepCallback, StepResult, Value, WorkerThreadContext};

pub trait SnapshotNextStepBuilder: Send {
    fn build(mut self: Box<Self>, snapshot: Box<storage::Snapshot>) -> Box<Step>;
}

pub trait SnapshotStep: Send + fmt::Display {
    fn create_next_step_builder(&self) -> Box<SnapshotNextStepBuilder>;
    fn get_request_context(&self) -> &kvrpcpb::Context;
}

impl<R: SnapshotStep> Step for R {
    #[inline]
    fn async_work(mut self: Box<Self>, context: &mut WorkerThreadContext, on_done: StepCallback) {
        let next_step_builder = self.create_next_step_builder();
        context.engine.async_snapshot(
            self.get_request_context(),
            box move |(_, snapshot_result)| match snapshot_result {
                Ok(snapshot) => {
                    let next_step = next_step_builder.build(snapshot);
                    on_done(StepResult::Continue(Box::from(next_step)));
                }
                Err(e) => {
                    on_done(StepResult::Finish(
                        Err(Error::StorageError(storage::Error::from(e))),
                    ));
                }
            },
        );
    }
}
