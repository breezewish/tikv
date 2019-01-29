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

#![cfg_attr(test, feature(test))]
#[cfg(test)]
extern crate test;

extern crate rand;
extern crate slog;
extern crate time;

extern crate tikv;
extern crate tikv_log;
extern crate tikv_logger;

mod kv_generator;
mod logging;
mod macros;
mod security;

use std::env;

pub use kv_generator::*;
pub use logging::*;
pub use macros::*;
pub use security::*;

pub fn setup_for_ci() {
    if env::var("CI").is_ok() && env::var("LOG_FILE").is_ok() {
        logging::init_log_for_test();
    }
    if env::var("PANIC_ABORT").is_ok() {
        // Panics as aborts, it's helpful for debugging,
        // but also stops tests immediately.
        tikv::util::set_panic_hook(true, "./");
    }

    // HACK! Always use epollex in tests on Linux.
    // See more: https://github.com/grpc/grpc/blob/v1.17.2/src/core/lib/iomgr/ev_posix.cc#L124
    #[cfg(target_os = "linux")]
    env::set_var("GRPC_POLL_STRATEGY", "epollex");

    tikv::util::check_environment_variables();
}
