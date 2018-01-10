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
use std::{fmt, sync};

use super::{Error, Step, StepCallback, StepResult, Value, WorkerThreadContext};

pub trait SnapshotNextStepBuilder: Send {
    fn build(self: Box<Self>, snapshot: Box<storage::Snapshot>) -> Box<Step>;
}

pub trait SnapshotStep: Send + fmt::Display {
    fn create_next_step_builder(&self) -> Box<SnapshotNextStepBuilder>;
    fn get_request_context(&self) -> &kvrpcpb::Context;
}

impl<R: SnapshotStep> Step for R {
    #[inline]
    fn async_work(self: Box<Self>, context: &mut WorkerThreadContext, on_done: StepCallback) {
        // TODO: replace sync::Arc::new(sync::Mutex::new(..)) with a flagged sync / send struct
        // to improve performance.
        let on_done = sync::Arc::new(sync::Mutex::new(Some(on_done)));
        let on_done_for_result = on_done.clone();
        let on_done_for_callback = on_done.clone();
        let next_step_builder = self.create_next_step_builder();
        let result = context.engine.async_snapshot(
            self.get_request_context(),
            box move |(_, snapshot_result)| {
                let mut on_done = on_done_for_callback.lock().unwrap();
                let on_done = on_done.take().unwrap();
                match snapshot_result {
                    Ok(snapshot) => {
                        let next_step = next_step_builder.build(snapshot);
                        on_done(StepResult::Continue(Box::from(next_step)));
                    }
                    Err(e) => {
                        on_done(StepResult::Finish(
                            Err(Error::Storage(storage::Error::from(e))),
                        ));
                    }
                }
            },
        );
        if let Err(e) = result {
            let mut on_done = on_done_for_result.lock().unwrap();
            let on_done = on_done.take().unwrap();
            on_done(StepResult::Finish(
                Err(Error::Storage(storage::Error::from(e))),
            ));
        }
    }
}
