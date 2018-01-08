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

use std::fmt;
use super::{Error, Step, StepCallback, StepResult, Value, WorkerThreadContext};
//use super::util::SnapshotStep;
use kvproto::kvrpcpb;
use storage;

pub struct KvGetStep {
    pub req: kvrpcpb::GetRequest,
}

impl fmt::Display for KvGetStep {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KvGet step 1, req = {:?}", self.req)
    }
}
//
//impl SnapshotStep for KvGetStep {
//    #[inline]
//    fn build_next_step(&self, snapshot: Box<storage::Snapshot>) -> Box<Step> {
//        box KvGetStepSecond {
//            snapshot,
//            req_context: self.req.take_context(),
//            key: storage::Key::from_raw(self.req.get_key()),
//            start_ts: self.req.get_version(),
//        }
//    }
//    #[inline]
//    fn get_request_context(&self) -> &kvrpcpb::Context {
//        &self.req.get_context()
//    }
//}

impl Step for KvGetStep {
    fn async_work(mut self: Box<Self>, context: &mut WorkerThreadContext, on_done: StepCallback) {
        println!("KvGetStep async_work");
        let req = self.req;
        let req_context = req.get_context();
        let isolation_level = req_context.get_isolation_level();
        let not_fill_cache = req_context.get_not_fill_cache();
        let key = storage::Key::from_raw(req.get_key());
        let start_ts = req.get_version();
        println!("KvGetStep async_work2");
        context.engine.async_snapshot(
            req_context,
            box move |(_, snapshot_result)| {
                println!("KvGetStep async_snapshot callback");
                match snapshot_result {
                    Ok(snapshot) => {
                        let next_step = box KvGetStepSecond {
                            snapshot: Some(snapshot),
                            isolation_level,
                            not_fill_cache,
                            key,
                            start_ts,
                        };
                        on_done(StepResult::Continue(next_step));
                    }
                    Err(e) => {
                        on_done(StepResult::Finish(Err(Error::SnapshotError)));
                    }
                }
            },
        );
    }
}

struct KvGetStepSecond {
    snapshot: Option<Box<storage::Snapshot>>,
    isolation_level: kvrpcpb::IsolationLevel,
    not_fill_cache: bool,
    key: storage::Key,
    start_ts: u64,
}

impl fmt::Display for KvGetStepSecond {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "KvGet step 2, isolation_level = {:?}, not_fill_cache = {}, key = {}, start_ts = {}",
            self.isolation_level,
            self.not_fill_cache,
            self.key,
            self.start_ts
        )
    }
}

impl Step for KvGetStepSecond {
    fn async_work(mut self: Box<Self>, context: &mut WorkerThreadContext, on_done: StepCallback) {
        println!("KvGetStepSecond async_work");
        let mut statistics = storage::Statistics::default();
        let snap_store = storage::SnapshotStore::new(
            self.snapshot.take().unwrap(),
            self.start_ts,
            self.isolation_level,
            !self.not_fill_cache,
        );
        let res = snap_store.get(&self.key, &mut statistics);
        // TODO
        on_done(StepResult::Finish(Ok(Value::Foo)));
    }
}
