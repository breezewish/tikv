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

pub fn slog_level_filter_to_log(filter: ::slog::FilterLevel) -> ::log::LevelFilter {
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
