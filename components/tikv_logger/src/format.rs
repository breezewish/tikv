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

use std::io;

use slog::{Drain, OwnedKVList, Record, KV};
use slog_term::{Decorator, RecordDecorator};

const TIMESTAMP_FORMAT: &str = "%Y/%m/%d %H:%M:%S%.3f %:z";

pub struct TikvFormat<D>
where
    D: Decorator,
{
    decorator: D,
}

impl<D> TikvFormat<D>
where
    D: Decorator,
{
    pub fn new(decorator: D) -> Self {
        Self { decorator }
    }
}

impl<D> Drain for TikvFormat<D>
where
    D: Decorator,
{
    type Ok = ();
    type Err = io::Error;

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        self.decorator.with_record(record, values, |decorator| {
            write_log_header(decorator, record)?;
            write_log_msg(decorator, record)?;
            write_log_fields(decorator, record, values)?;

            decorator.start_whitespace()?;
            writeln!(decorator)?;

            decorator.flush()?;

            Ok(())
        })
    }
}

struct Serializer<'a> {
    decorator: &'a mut RecordDecorator,
}

impl<'a> Serializer<'a> {
    fn new(decorator: &'a mut RecordDecorator) -> Self {
        Serializer { decorator }
    }

    fn write_whitespace(&mut self) -> io::Result<()> {
        self.decorator.start_whitespace()?;
        write!(self.decorator, " ")?;
        Ok(())
    }

    fn finish(self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> Drop for Serializer<'a> {
    fn drop(&mut self) {}
}

#[allow(clippy::write_literal)]
impl<'a> slog::ser::Serializer for Serializer<'a> {
    fn emit_none(&mut self, key: slog::Key) -> slog::Result {
        self.emit_arguments(key, &format_args!("None"))
    }

    fn emit_arguments(&mut self, key: slog::Key, val: &::std::fmt::Arguments) -> slog::Result {
        self.write_whitespace()?;

        // Write key
        write!(self.decorator, "[")?;
        self.decorator.start_key()?;
        write_escaped_str(&mut self.decorator, key as &str)?;

        // Write separator
        self.decorator.start_separator()?;
        write!(self.decorator, "=")?;

        // Write value
        let value = format!("{}", val);
        self.decorator.start_value()?;
        write_escaped_str(self.decorator, &value)?;
        self.decorator.reset()?;
        write!(self.decorator, "]")?;
        Ok(())
    }
}

/// Writes log header to decorator. See [log-header](https://github.com/tikv/rfcs/blob/master/text/2018-12-19-unified-log-format.md#log-header-section)
fn write_log_header(decorator: &mut RecordDecorator, record: &Record) -> io::Result<()> {
    decorator.start_timestamp()?;
    write!(
        decorator,
        "[{}]",
        chrono::Local::now().format(TIMESTAMP_FORMAT)
    )?;

    decorator.start_whitespace()?;
    write!(decorator, " ")?;

    decorator.start_level()?;
    write!(
        decorator,
        "[{}]",
        ::tikv_log::util::level_to_unified_level_string(record.level())
    )?;

    decorator.start_whitespace()?;
    write!(decorator, " ")?;

    // Writes source file info.
    decorator.start_msg()?; // There is no `start_file()` or `start_line()`.
    if let Some(path) = ::std::path::Path::new(record.file())
        .file_name()
        .and_then(|path| path.to_str())
    {
        write!(decorator, "[")?;
        write_file_name(decorator, path)?;
        write!(decorator, ":{}]", record.line())?
    } else {
        write!(decorator, "[<unknown>]")?
    }

    Ok(())
}

/// Writes log message to decorator. See [log-message](https://github.com/tikv/rfcs/blob/master/text/2018-12-19-unified-log-format.md#log-message-section)
#[inline]
fn write_log_msg(decorator: &mut RecordDecorator, record: &Record) -> io::Result<()> {
    decorator.start_whitespace()?;
    write!(decorator, " ")?;

    decorator.start_msg()?;
    write!(decorator, "[")?;
    let msg = format!("{}", record.msg());
    write_escaped_str(decorator, &msg)?;
    write!(decorator, "]")?;

    Ok(())
}

/// Writes log fields to decorator. See [log-fields](https://github.com/tikv/rfcs/blob/master/text/2018-12-19-unified-log-format.md#log-fields-section)
#[inline]
fn write_log_fields(
    decorator: &mut RecordDecorator,
    record: &Record,
    values: &OwnedKVList,
) -> io::Result<()> {
    let mut serializer = Serializer::new(decorator);

    record.kv().serialize(record, &mut serializer)?;

    values.serialize(record, &mut serializer)?;

    serializer.finish()?;

    Ok(())
}

/// Writes file name into the writer, removes the character which not match `[a-zA-Z0-9\.-_]`
fn write_file_name<W>(writer: &mut W, file_name: &str) -> io::Result<()>
where
    W: io::Write + ?Sized,
{
    let mut start = 0;
    let bytes = file_name.as_bytes();
    for (index, &b) in bytes.iter().enumerate() {
        if (b >= b'A' && b <= b'Z')
            || (b >= b'a' && b <= b'z')
            || (b >= b'0' && b <= b'9')
            || b == b'.'
            || b == b'-'
            || b == b'_'
        {
            continue;
        }
        if start < index {
            writer.write_all((&file_name[start..index]).as_bytes())?;
        }
        start = index + 1;
    }
    if start < bytes.len() {
        writer.write_all((&file_name[start..]).as_bytes())?;
    }
    Ok(())
}

/// According to [RFC: Unified Log Format], it returns `true` when this byte stream contains
/// the following characters, which means this input stream needs to be JSON encoded.
/// Otherwise, it returns `false`.
///
/// - U+0000 (NULL) ~ U+0020 (SPACE)
/// - U+0022 (QUOTATION MARK)
/// - U+003D (EQUALS SIGN)
/// - U+005B (LEFT SQUARE BRACKET)
/// - U+005D (RIGHT SQUARE BRACKET)
///
/// [RFC: Unified Log Format]: (https://github.com/tikv/rfcs/blob/master/text/2018-12-19-unified-log-format.md)
///
#[inline]
fn need_json_encode(bytes: &[u8]) -> bool {
    for &byte in bytes {
        if byte <= 0x20 || byte == 0x22 || byte == 0x3D || byte == 0x5B || byte == 0x5D {
            return true;
        }
    }
    false
}

/// According to [RFC: Unified Log Format], escapes the given data and writes it into a writer.
/// If there is no character [`need json encode`], it writes the data into the writer directly.
/// Else, it serializes the given data structure as JSON into a writer.
///
/// [RFC: Unified Log Format]: (https://github.com/tikv/rfcs/blob/master/text/2018-12-19-unified-log-format.md)
/// [`need json encode`]: #method.need_json_encode
#[inline]
fn write_escaped_str<W>(writer: &mut W, value: &str) -> io::Result<()>
where
    W: io::Write + ?Sized,
{
    if !need_json_encode(value.as_bytes()) {
        writer.write_all(value.as_bytes())?;
    } else {
        ::serde_json::to_writer(writer, value)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io;

    #[test]
    fn test_need_json_encode() {
        let cases = [
            ("abc", false),
            ("a b", true),
            ("a=b", true),
            ("a[b", true),
            ("a]b", true),
            ("\u{000f}", true),
            ("💖", false),
            ("�", false),
            ("\u{7fff}\u{ffff}", false),
            ("欢迎", false),
            ("欢迎 TiKV", true),
        ];
        for (input, expect) in &cases {
            assert_eq!(
                need_json_encode(input.as_bytes()),
                *expect,
                "{} | {}",
                input,
                expect
            );
        }
    }

    #[test]
    fn test_write_file_name() {
        let mut s = vec![];
        write_file_name(
            &mut s,
            "+=!@#$%^&*(){}|:\"<>/?\u{000f} 老虎 tiger 🐅\r\n\\-_1234567890.rs",
        )
        .unwrap();
        assert_eq!("tiger-_1234567890.rs", &String::from_utf8(s).unwrap())
    }

    // Due to the requirements of `Logger::root*` on a writer with a 'static lifetime
    // we need to make a Thread Local,
    // and implement a custom writer.
    thread_local! {
        static BUFFER: ::std::cell::RefCell<Vec<u8>> = ::std::cell::RefCell::new(Vec::new());
    }
    struct TestWriter;
    impl io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            BUFFER.with(|buffer| buffer.borrow_mut().write(buf))
        }
        fn flush(&mut self) -> io::Result<()> {
            BUFFER.with(|buffer| buffer.borrow_mut().flush())
        }
    }

    #[test]
    fn test_tikv_format() {
        use std::time::Duration;

        let decorator = ::slog_term::PlainSyncDecorator::new(TestWriter);
        let drain = TikvFormat::new(decorator).fuse();
        let logger = ::slog::Logger::root_typed(drain, ::slog::slog_o!());

        // Empty message is not recommend, just for test purpose here.
        ::slog::slog_info!(logger, "");
        ::slog::slog_info!(logger, "Welcome");
        ::slog::slog_info!(logger, "Welcome TiKV");
        ::slog::slog_info!(logger, "欢迎");
        ::slog::slog_info!(logger, "欢迎 TiKV");

        ::slog::slog_info!(logger, "failed to fetch URL";
                    "url" => "http://example.com",
                    "attempt" => 3,
                    "backoff" => ?Duration::new(3, 0),
        );

        ::slog::slog_info!(
            logger,
            "failed to \"fetch\" [URL]: {}",
            "http://example.com"
        );

        ::slog::slog_debug!(logger, "Slow query";
            "sql" => "SELECT * FROM TABLE WHERE ID=\"abc\"",
            "duration" => ?Duration::new(0, 123),
            "process keys" => 1500,
        );

        ::slog::slog_warn!(logger, "Type";
            "Counter" => ::std::f64::NAN,
            "Score" => ::std::f64::INFINITY,
            "Other" => ::std::f64::NEG_INFINITY
        );

        let none: Option<u8> = None;
        ::slog::slog_info!(logger, "more type tests";
            "field1" => "no_quote",
            "field2" => "in quote",
            "urls" => ?["http://xxx.com:2347", "http://xxx.com:2432"],
            "url-peers" => ?["peer1", "peer 2"],
            "store ids" => ?[1, 2, 3],
            "is_true" => true,
            "is_false" => false,
            "is_None" => none,
            "u8" => 34 as u8,
            "str_array" => ?["💖",
                "�",
                "☺☻☹",
                "日a本b語ç日ð本Ê語þ日¥本¼語i日©",
                "日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©",
                "\\x80\\x80\\x80\\x80",
                "<car><mirror>XML</mirror></car>"]
        );

        let expect = r#"
[2019/01/15 13:40:39.619 +08:00] [INFO] [format.rs:469] []
[2019/01/15 13:40:39.619 +08:00] [INFO] [format.rs:469] [Welcome]
[2019/01/15 13:40:39.619 +08:00] [INFO] [format.rs:470] ["Welcome TiKV"]
[2019/01/15 13:40:39.619 +08:00] [INFO] [format.rs:471] [欢迎]
[2019/01/15 13:40:39.619 +08:00] [INFO] [format.rs:472] ["欢迎 TiKV"]
[2019/01/15 13:40:39.615 +08:00] [INFO] [format.rs:455] ["failed to fetch URL"] [backoff=3s] [attempt=3] [url=http://example.com]
[2019/01/15 13:40:39.619 +08:00] [INFO] [format.rs:460] ["failed to \"fetch\" [URL]: http://example.com"]
[2019/01/15 13:40:39.619 +08:00] [DEBUG] [format.rs:463] ["Slow query"] ["process keys"=1500] [duration=123ns] [sql="SELECT * FROM TABLE WHERE ID=\"abc\""]
[2019/01/15 13:40:39.619 +08:00] [WARN] [format.rs:473] [Type] [Other=-inf] [Score=inf] [Counter=NaN]
[2019/01/16 16:56:04.854 +08:00] [INFO] [format.rs:391] ["more type tests"] [str_array="[\"💖\", \"�\", \"☺☻☹\", \"日a本b語ç日ð本Ê語þ日¥本¼語i日©\", \"日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©\", \"\\\\x80\\\\x80\\\\x80\\\\x80\", \"<car><mirror>XML</mirror></car>\"]"] [u8=34] [is_None=None] [is_false=false] [is_true=true] ["store ids"="[1, 2, 3]"] [url-peers="[\"peer1\", \"peer 2\"]"] [urls="[\"http://xxx.com:2347\", \"http://xxx.com:2432\"]"] [field2="in quote"] [field1=no_quote]
"#;
        let expect = expect.trim();

        BUFFER.with(|buffer| {
            let buffer = buffer.borrow_mut();
            let output = ::std::str::from_utf8(&*buffer).unwrap().trim();
            assert_eq!(output.lines().count(), expect.lines().count());

            let re = ::regex::Regex::new(r"^\[(?P<datetime>.*?)\] \[(?P<level>.*?)\] \[(?P<source_file>.*?)\] (?P<remaining>[\s\S]*)$").unwrap();

            for (output_line, expect_line) in output.lines().zip(expect.lines()) {
                let expect_segments = re.captures(expect_line).unwrap();
                let output_segments = re.captures(output_line).unwrap();

                assert!(validate_log_datetime(&expect_segments["datetime"]));
                assert!(validate_log_source_file(
                    &expect_segments["source_file"],
                    &output_segments["source_file"]
                ));
                assert_eq!(expect_segments["level"], output_segments["level"]);
                assert_eq!(expect_segments["remaining"], output_segments["remaining"]);
            }
        });
    }

    /// Validates whether source file output matches the expectation.
    ///
    /// Line numbers will be ignored and others will be checked.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(true, validate_log_source_file("<unknown>", "<unknown>"));
    /// assert_eq!(true, validate_log_source_file("mod.rs:1", "mod.rs:1"));
    /// assert_eq!(true, validate_log_source_file("mod.rs:1", "mod.rs:100"));
    /// assert_eq!(false, validate_log_source_file("mod.rs:1", "<unknown>"));
    /// assert_eq!(false, validate_log_source_file("mod.rs:1", "mod.rs:NAN"));
    /// ```
    fn validate_log_source_file(output: &str, expect: &str) -> bool {
        if expect.eq(output) {
            return true;
        }
        if expect.eq("<unknown>") || output.eq("<unknown>") {
            return false;
        }

        let mut iter = expect.split(':').zip(output.split(':'));
        let (expect_file_name, output_file_name) = iter.next().unwrap();
        assert_eq!(expect_file_name, output_file_name);

        let (_expect_line_number, output_line_number) = iter.next().unwrap();
        output_line_number.parse::<usize>().is_ok()
    }

    /// Validates whether date time output is a valid date time string.
    fn validate_log_datetime(datetime: &str) -> bool {
        ::chrono::DateTime::parse_from_str(datetime, TIMESTAMP_FORMAT).is_ok()
    }
}
