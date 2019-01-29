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

// We use `slog-global` provided by this crate, so that users don't need to introduce `slog-global`
// any more in `extern crate`s and `Cargo.toml`.

/// Logs a critical level message.
#[macro_export]
macro_rules! crit( ($($args:tt)+) => {
    if $crate::is_level_enabled(::slog::Level::Critical) {
        $crate::slog_global::crit!($($args)+)
    }
};);

/// Logs a error level message.
#[macro_export]
macro_rules! error( ($($args:tt)+) => {
    if $crate::is_level_enabled(::slog::Level::Error) {
        $crate::slog_global::error!($($args)+)
    }
};);

/// Logs a warning level message.
#[macro_export]
macro_rules! warn( ($($args:tt)+) => {
    if $crate::is_level_enabled(::slog::Level::Warning) {
        $crate::slog_global::warn!($($args)+)
    }
};);

/// Logs a info level message.
#[macro_export]
macro_rules! info( ($($args:tt)+) => {
    if $crate::is_level_enabled(::slog::Level::Info) {
        $crate::slog_global::info!($($args)+)
    }
};);

/// Logs a debug level message.
#[macro_export]
macro_rules! debug( ($($args:tt)+) => {
    if $crate::is_level_enabled(::slog::Level::Debug) {
        $crate::slog_global::debug!($($args)+)
    }
};);

/// Logs a trace level message.
#[macro_export]
macro_rules! trace( ($($args:tt)+) => {
    if $crate::is_level_enabled(::slog::Level::Trace) {
        $crate::slog_global::trace!($($args)+)
    }
};);
