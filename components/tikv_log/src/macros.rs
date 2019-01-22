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

/// Logs a critical level message.
#[macro_export]
macro_rules! crit( ($($args:tt)+) => {
    let level = slog::Level::Critical.as_usize();
    if level <= slog::__slog_static_max_level().as_usize() && level <= $crate::max_level_usize() {
        ::slog_global::crit!($($args)+)
    }
};);

/// Logs a error level message.
#[macro_export]
macro_rules! error( ($($args:tt)+) => {
    let level = slog::Level::Error.as_usize();
    if level <= slog::__slog_static_max_level().as_usize() && level <= $crate::max_level_usize() {
        ::slog_global::error!($($args)+)
    }
};);

/// Logs a warning level message.
#[macro_export]
macro_rules! warn( ($($args:tt)+) => {
    let level = slog::Level::Warning.as_usize();
    if level <= slog::__slog_static_max_level().as_usize() && level <= $crate::max_level_usize() {
        ::slog_global::warn!($($args)+)
    }
};);

/// Logs a info level message.
#[macro_export]
macro_rules! info( ($($args:tt)+) => {
    let level = slog::Level::Info.as_usize();
    if level <= slog::__slog_static_max_level().as_usize() && level <= $crate::max_level_usize() {
        ::slog_global::info!($($args)+)
    }
};);

/// Logs a debug level message.
#[macro_export]
macro_rules! debug( ($($args:tt)+) => {
    let level = slog::Level::Debug.as_usize();
    if level <= slog::__slog_static_max_level().as_usize() && level <= $crate::max_level_usize() {
        ::slog_global::debug!($($args)+)
    }
};);

/// Logs a trace level message.
#[macro_export]
macro_rules! trace( ($($args:tt)+) => {
    let level = slog::Level::Trace.as_usize();
    if level <= slog::__slog_static_max_level().as_usize() && level <= $crate::max_level_usize() {
        ::slog_global::trace!($($args)+)
    }
};);
