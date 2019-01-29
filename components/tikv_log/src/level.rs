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

use std::sync::atomic::{AtomicUsize, Ordering};

use slog::{FilterLevel, Level};

lazy_static! {
    pub static ref MAX_LOG_LEVEL_FILTER: AtomicUsize =
        AtomicUsize::new(FilterLevel::max().as_usize());
}

/// Gets current run-time max level filter in `usize`.
///
/// Generally this is an internal API. Consider using `max_level()` instead.
#[doc(hidden)]
#[inline]
pub fn max_level_usize() -> usize {
    MAX_LOG_LEVEL_FILTER.load(Ordering::Relaxed)
}

/// Gets current run-time max level filter.
#[inline]
pub fn max_level() -> FilterLevel {
    FilterLevel::from_usize(max_level_usize()).unwrap()
}

/// Sets run-time max level filter for macros from this crate.
///
/// Notice that log outputs are still restricted by compile-time max level filters.
#[inline]
pub fn set_max_level(level: FilterLevel) {
    let level_u = level.as_usize();
    MAX_LOG_LEVEL_FILTER.store(level_u, Ordering::SeqCst);
}

/// Returns whether the specified logging level can be outputted.
#[inline]
pub fn is_level_enabled(level: Level) -> bool {
    let level_u = level.as_usize();
    level_u <= ::slog::__slog_static_max_level().as_usize() && level_u <= ::max_level_usize()
}
