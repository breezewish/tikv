// Copyright 2019 PingCAP, Inc.
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

//! Provides global log macros that supports run-time log level filter over slog-global.
//!
//! Generally, this crate is used at places where there are log demands, e.g. libraries.
//! Notice that a logger should be set up to actually output these logs. See `tikv_logger`.
//!
//! Internally, this crate uses slog. Due to macro expansion limitations, users need to import
//! slog as well.

// TODO: Make this crate working in maximum building environments, e.g. WASM and no-std, so that
// libraries rely on this crate will not be affected.

#[macro_use]
extern crate lazy_static;
extern crate slog;
pub extern crate slog_global;

#[macro_use]
mod macros;
mod level;
pub mod util;

pub use self::level::*;
