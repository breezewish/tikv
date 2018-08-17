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

use kvproto::kvrpcpb::Context;

use super::sync_storage::SyncStorage;
use tikv::server::readpool::{self, ReadPool};
use tikv::storage;
use tikv::storage::engine::RocksEngine;
use tikv::util::worker::FutureWorker;

pub fn new_storage(path: &str) -> SyncStorage<RocksEngine> {
    let pd_worker = FutureWorker::new("test-pd-worker");
    let read_pool = ReadPool::new(
        "readpool",
        &readpool::Config::default_with_concurrency(1),
        || || storage::ReadPoolContext::new(pd_worker.scheduler()),
    );
    let mut config = storage::Config::default();
    config.data_dir = path.to_owned();
    SyncStorage::new(&Default::default(), read_pool)
}

pub fn new_no_cache_context() -> Context {
    let mut ctx = Context::new();
    ctx.set_not_fill_cache(true);
    ctx
}
