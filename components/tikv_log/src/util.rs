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

use slog::Level;

/// Converts from a human readable full level string.
pub fn full_string_to_level(lv: &str) -> Option<Level> {
    let lower_cased = lv.to_owned().to_lowercase();
    match lower_cased.as_str() {
        "critical" => Some(Level::Critical),
        "error" => Some(Level::Error),
        // We support `warn` due to legacy.
        "warning" | "warn" => Some(Level::Warning),
        "debug" => Some(Level::Debug),
        "trace" => Some(Level::Trace),
        "info" => Some(Level::Info),
        _ => None,
    }
}

/// Converts to a human readable full level string.
pub fn level_to_full_string(lv: Level) -> &'static str {
    match lv {
        Level::Critical => "critical",
        Level::Error => "error",
        Level::Warning => "warning",
        Level::Debug => "debug",
        Level::Trace => "trace",
        Level::Info => "info",
    }
}

/// Converts to a level string stipulated by Unified Log Format RFC.
pub fn level_to_unified_level_string(lv: Level) -> &'static str {
    match lv {
        Level::Critical => "FATAL",
        Level::Error => "ERROR",
        Level::Warning => "WARN",
        Level::Info => "INFO",
        Level::Debug => "DEBUG",
        Level::Trace => "TRACE",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_string_to_level() {
        // Ensure UPPER, Capitalized, and lower case all map over.
        assert_eq!(Some(Level::Trace), full_string_to_level("TRACE"));
        assert_eq!(Some(Level::Trace), full_string_to_level("Trace"));
        assert_eq!(Some(Level::Trace), full_string_to_level("trace"));
        // Due to legacy we need to ensure that `warn` maps to `Warning`.
        assert_eq!(Some(Level::Warning), full_string_to_level("warn"));
        assert_eq!(Some(Level::Warning), full_string_to_level("warning"));
        // Ensure that all non-defined values map to `Info`.
        assert_eq!(None, full_string_to_level("Off"));
        assert_eq!(None, full_string_to_level("definitely not an option"));
    }
}
