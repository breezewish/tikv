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

use storage;
use kvproto::kvrpcpb;
use std::fmt;

use super::{Error, Step, StepCallback, StepResult, Value, WorkerThreadContext};
//
//pub trait SnapshotStep {
//    fn build_next_step(self, snapshot: Box<storage::Snapshot>) -> Box<Step>;
//    fn get_request_context(&self) -> &kvrpcpb::Context;
//}
//
//impl<R: SnapshotStep + Send + fmt::Display> Step for R {
//    #[inline]
//    fn async_work(self, context: &mut WorkerThreadContext, on_done: StepCallback) {
//        context.engine.async_snapshot(
//            self.get_request_context(),
//            box move |(_, snapshot_result)| match snapshot_result {
//                Ok(snapshot) => {
//                    let next_step = self.build_next_step(snapshot);
//                    on_done(StepResult::Continue(next_step));
//                }
//                Err(e) => {
//                    on_done(StepResult::Finish(Err(Error::SnapshotError)));
//                }
//            },
//        );
//    }
//}
