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
use std::{cell, fmt, sync};

use super::{Error, Step, StepCallback, StepResult, Value, WorkerThreadContext};

pub trait SnapshotNextStepBuilder: Send {
    fn build(self: Box<Self>, snapshot: Box<storage::Snapshot>) -> Box<Step>;
}

pub trait SnapshotStep: Send + fmt::Display {
    fn create_next_step_builder(&self) -> Box<SnapshotNextStepBuilder>;
    fn get_request_context(&self) -> &kvrpcpb::Context;
}

/// An cell that only ensures safety when its content is taken only once. Its reference
/// can be shared between multiple threads.
struct UnsafeOnetimeCell<T> {
    val: cell::Cell<Option<T>>,
}

impl<T> UnsafeOnetimeCell<T> {
    fn new(v: T) -> UnsafeOnetimeCell<T> {
        UnsafeOnetimeCell {
            val: cell::Cell::new(Some(v)),
        }
    }
    /// Should be only called once.
    fn take_once(&self) -> T {
        self.val.take().unwrap()
    }
}

unsafe impl<T> Sync for UnsafeOnetimeCell<T> {}

impl<R: SnapshotStep> Step for R {
    #[inline]
    fn async_work(self: Box<Self>, context: &mut WorkerThreadContext, on_done: StepCallback) {
        let on_done = sync::Arc::new(UnsafeOnetimeCell::new(on_done));
        let on_done_for_result = on_done.clone();
        let on_done_for_callback = on_done.clone();
        let next_step_builder = self.create_next_step_builder();
        let result = context.engine.async_snapshot(
            self.get_request_context(),
            box move |(_, snapshot_result)| match snapshot_result {
                Ok(snapshot) => {
                    let next_step = next_step_builder.build(snapshot);
                    (on_done_for_callback.take_once())(StepResult::Continue(Box::from(next_step)));
                }
                Err(e) => {
                    (on_done_for_callback.take_once())(StepResult::Finish(
                        Err(Error::Storage(storage::Error::from(e))),
                    ));
                }
            },
        );
        // TODO: Test whether this actually works
        if let Err(e) = result {
            (on_done_for_result.take_once())(StepResult::Finish(
                Err(Error::Storage(storage::Error::from(e))),
            ));
        }
    }
}
