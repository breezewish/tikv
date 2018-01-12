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

use std::fmt;
use super::{Error, SubTask, SubTaskCallback, SubTaskResult, Value, WorkerThreadContext};
use super::util::{SnapshotNextSubTaskBuilder, SnapshotSubTask};
use kvproto::kvrpcpb;
use storage;

pub struct KvGet {
    pub req_context: kvrpcpb::Context,
    pub key: Vec<u8>,
    pub start_ts: u64,
}

impl fmt::Display for KvGet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KvGet[1]")
    }
}

impl SnapshotSubTask for KvGet {
    #[inline]
    fn new_next_subtask_builder(&self) -> Box<SnapshotNextSubTaskBuilder> {
        box KvGetSubTaskSecondBuilder {
            isolation_level: self.req_context.get_isolation_level(),
            not_fill_cache: self.req_context.get_not_fill_cache(),
            key: storage::Key::from_raw(self.key.as_slice()),
            start_ts: self.start_ts,
        }
    }
    #[inline]
    fn get_request_context(&self) -> &kvrpcpb::Context {
        &self.req_context
    }
}

struct KvGetSubTaskSecondBuilder {
    isolation_level: kvrpcpb::IsolationLevel,
    not_fill_cache: bool,
    key: storage::Key,
    start_ts: u64,
}

impl SnapshotNextSubTaskBuilder for KvGetSubTaskSecondBuilder {
    fn build(self: Box<Self>, snapshot: Box<storage::Snapshot>) -> Box<SubTask> {
        box KvGetSubTaskSecond {
            snapshot: Some(snapshot),
            builder: self,
        }
    }
}

struct KvGetSubTaskSecond {
    snapshot: Option<Box<storage::Snapshot>>,
    builder: Box<KvGetSubTaskSecondBuilder>,
}

impl fmt::Display for KvGetSubTaskSecond {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KvGet[2]")
    }
}

impl SubTask for KvGetSubTaskSecond {
    fn async_work(
        mut self: Box<Self>,
        _context: &mut WorkerThreadContext,
        on_done: SubTaskCallback,
    ) {
        let mut statistics = storage::Statistics::default();
        let snap_store = storage::SnapshotStore::new(
            self.snapshot.take().unwrap(),
            self.builder.start_ts,
            self.builder.isolation_level,
            !self.builder.not_fill_cache,
        );
        let res = snap_store.get(&self.builder.key, &mut statistics);
        on_done(SubTaskResult::Finish(match res {
            Ok(val) => Ok(Value::StorageValue(val)),
            Err(e) => Err(Error::Storage(storage::Error::from(e))),
        }));
    }
}
