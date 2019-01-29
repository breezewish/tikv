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

//! Provides loggers that conforms TiKV Unified Log Format RFC.
//!
//! Generally, this crate is used at places where logs need to be outputed, e.g. binaries.
//!
//! Additionally, this crate provides a `fatal!` macro, which is roughly `crit!` with process exit.

extern crate chrono;
extern crate log;
extern crate panic_hook;
#[cfg(test)]
extern crate regex;
extern crate serde_json;
extern crate slog;
extern crate slog_async;
extern crate slog_term;
#[cfg(test)]
extern crate tempdir;
extern crate tikv_log;
#[cfg(test)]
extern crate utime;

mod file_logger;
mod format;

use std::io::{self, BufWriter};
use std::path::Path;
use std::sync::Mutex;

use chrono::Duration;
use slog::Drain;
use slog_async::{Async, OverflowStrategy};
use slog_term::{PlainDecorator, TermDecorator};

use self::file_logger::RotatingFileLogger;
use self::format::TikvFormat;

// Default is 128.
// Extended since blocking is set, and we don't want to block very often.
const SLOG_CHANNEL_SIZE: usize = 10240;

// Default is DropAndReport.
// It is not desirable to have dropped logs in our use case.
const SLOG_CHANNEL_OVERFLOW_STRATEGY: OverflowStrategy = OverflowStrategy::Block;

/// A simple alias to `PlainDecorator<BufWriter<RotatingFileLogger>>`.
pub type RotatingFileDecorator = PlainDecorator<BufWriter<RotatingFileLogger>>;

/// Initializes the logging facility. This function should be called only once.
///
/// After this function is called, logs from the log crate and slog crate will be available
/// and printed to the stderr.
pub fn init() {
    let drainer = build_stderr_drainer();
    use_drainer_sync(drainer);

    // Let's just discard set log error.
    let _ = tikv_log::slog_global::redirect_std_log(None);
}

/// Uses the specified drain to build a global sync logger.
pub fn use_drainer_sync<D>(drain: D)
where
    D: slog::Drain + Send + 'static,
    <D as slog::Drain>::Err: ::std::fmt::Debug,
{
    let drain = Async::new(drain.fuse())
        .chan_size(SLOG_CHANNEL_SIZE)
        .overflow_strategy(SLOG_CHANNEL_OVERFLOW_STRATEGY)
        .build()
        // .filter_level(level)    // FIXME
        .fuse();
    let logger = slog::Logger::root(drain, ::slog::slog_o!());
    tikv_log::slog_global::set_global(logger);
}

/// Uses the specified drain to build a global async logger.
pub fn use_drainer_async<D>(drain: D)
where
    D: slog::Drain + Send + 'static,
    <D as slog::Drain>::Err: ::std::fmt::Debug,
{
    let drain = Mutex::new(drain)
        // .filter_level(level)    // FIXME
        .fuse();
    let logger = slog::Logger::root(drain, ::slog::slog_o!());
    tikv_log::slog_global::set_global(logger);
}

pub fn clear_drainer() {
    // TODO
}

pub fn set_filter_level(level: ::slog::FilterLevel) {
    tikv_log::set_max_level(level);
    log::set_max_level(slog_level_filter_to_log(level));
    // TODO
    unimplemented!();
}

/// Builds a new file drainer which outputs log to a file at the specified path. The file
/// drainer rotates for the specified time span.
pub fn build_file_drainer(
    path: impl AsRef<Path>,
    rotation_timespan: Duration,
) -> io::Result<TikvFormat<RotatingFileDecorator>> {
    let logger = BufWriter::new(RotatingFileLogger::new(path, rotation_timespan)?);
    let decorator = PlainDecorator::new(logger);
    let drain = TikvFormat::new(decorator);
    Ok(drain)
}

/// Builds a new terminal drainer which outputs logs to stderr.
pub fn build_stderr_drainer() -> TikvFormat<TermDecorator> {
    let decorator = TermDecorator::new().stderr().build();
    TikvFormat::new(decorator)
}

/// Converts slog crate level filter to log crate level filter.
fn slog_level_filter_to_log(filter: ::slog::FilterLevel) -> ::log::LevelFilter {
    match filter {
        ::slog::FilterLevel::Off => ::log::LevelFilter::Off,
        ::slog::FilterLevel::Critical => ::log::LevelFilter::Error,
        ::slog::FilterLevel::Error => ::log::LevelFilter::Error,
        ::slog::FilterLevel::Warning => ::log::LevelFilter::Warn,
        ::slog::FilterLevel::Info => ::log::LevelFilter::Info,
        ::slog::FilterLevel::Debug => ::log::LevelFilter::Debug,
        ::slog::FilterLevel::Trace => ::log::LevelFilter::Trace,
    }
}

/// Logs a critical level message and exits process.
///
/// NOTICE: if `init()` is not called previously, the message will not be printed out.
#[macro_export]
macro_rules! fatal( ($($args:tt)+) => {
    {
        // TODO: init on demand so that calling `init()` is not required.
        $crate::use_drainer_sync($crate::build_stderr_drainer());
        ::tikv_log::crit!($($args)+);
        process::exit(1)
    }
};);
