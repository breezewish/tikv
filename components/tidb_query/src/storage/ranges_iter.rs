// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::iter::Peekable;
use std::vec::IntoIter;

use super::range::{IntervalRange, PointRange, Range};

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum IterStatus {
    /// All ranges are consumed.
    Drained,

    /// Last range is drained or this iteration is a fresh start so that caller should scan
    /// on a new range.
    NewIntervalRange(IntervalRange),
    NewMultiPointRange(Vec<PointRange>),

    /// Last interval range is not drained and the caller should continue scanning without changing
    /// the scan range.
    Continue,
}

/// An iterator like structure that produces user key ranges.
///
/// For each `next()`, it produces one of the following:
/// - a new range
/// - a flag indicating continuing last interval range or last multi-point range
/// - a flag indicating that all ranges are consumed
///
/// If a new range is returned, caller can then scan unknown amount of key(s) within this new range.
/// The caller must inform the structure so that it will emit a new range next time by calling
/// `notify_drained()` after current range is drained. Multiple `notify_drained()` without `next()`
/// will have no effect.
pub struct RangesIterator {
    /// Whether or not we are processing a valid range. If we are not processing a range, or there
    /// is no range any more, this field is `false`.
    in_range: bool,

    iter: Peekable<IntoIter<Range>>,
}

impl RangesIterator {
    #[inline]
    pub fn new(user_key_ranges: Vec<Range>) -> Self {
        Self {
            in_range: false,
            iter: user_key_ranges.into_iter().peekable(),
        }
    }

    /// Continues iterating.
    pub fn next(&mut self) -> IterStatus {
        if self.in_range {
            return IterStatus::Continue;
        }
        let mut point_ranges = Vec::with_capacity(128);
        loop {
            if !point_ranges.is_empty() {
                // If there exists point ranges, use peek to check if continuous
                // point range is finished.
                match self.iter.peek() {
                    Some(Range::Interval(_)) | None => {
                        // Finished
                        self.in_range = true;
                        return IterStatus::NewMultiPointRange(point_ranges);
                    }
                    _ => {}
                }
            }
            match self.iter.next() {
                None => {
                    return IterStatus::Drained;
                }
                Some(Range::Point(pr)) => {
                    point_ranges.push(pr);
                }
                Some(Range::Interval(ir)) => {
                    self.in_range = true;
                    return IterStatus::NewIntervalRange(ir);
                }
            }
        }
    }

    /// Notifies that current range is drained.
    #[inline]
    pub fn notify_drained(&mut self) {
        self.in_range = false;
    }
}

#[cfg(test)]
mod tests {
    use super::super::range::IntervalRange;
    use super::*;

    use byteorder::{BigEndian, WriteBytesExt};

    fn new_ir(serial: &mut u64) -> Range {
        let mut r = IntervalRange::from(("", ""));
        r.lower_inclusive.write_u64::<BigEndian>(*serial).unwrap();
        r.upper_exclusive
            .write_u64::<BigEndian>(*serial + 2)
            .unwrap();
        *serial += 3;
        Range::Interval(r)
    }

    fn new_pr(serial: &mut u64) -> Range {
        let mut r = PointRange::from("");
        r.0.write_u64::<BigEndian>(*serial).unwrap();
        *serial += 1;
        Range::Point(r)
    }

    fn build_ranges(pattern: &'static str) -> Vec<Range> {
        let mut r = Vec::new();
        let mut serial = 0;
        for c in pattern.chars() {
            match c {
                'i' => r.push(new_ir(&mut serial)),
                'p' => r.push(new_pr(&mut serial)),
                _ => unreachable!(),
            }
        }
        r
    }

    fn assert_it_ir(r: IterStatus, ranges: &[Range], index: usize) {
        match &ranges[index] {
            Range::Interval(ir) => {
                assert_eq!(r, IterStatus::NewIntervalRange(ir.clone()));
            }
            _ => panic!("The range provided is not an interval range"),
        }
    }

    fn assert_it_pr(r: IterStatus, ranges: &[Range], indexes: &[usize]) {
        let mut v = Vec::new();
        for index in indexes {
            match &ranges[*index] {
                Range::Point(pr) => {
                    v.push(pr.clone());
                }
                _ => panic!("The range provided is not a point range at index {}", index),
            }
        }
        assert_eq!(r, IterStatus::NewMultiPointRange(v));
    }

    #[test]
    fn test_interval() {
        // Empty
        let mut c = RangesIterator::new(vec![]);
        assert_eq!(c.next(), IterStatus::Drained);
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
        assert_eq!(c.next(), IterStatus::Drained);

        // Non-empty
        let ranges = build_ranges("iii");
        let mut c = RangesIterator::new(ranges.clone());
        assert_it_ir(c.next(), &ranges, 0);
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_it_ir(c.next(), &ranges, 1);
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        c.notify_drained(); // multiple consumes will not take effect
        assert_it_ir(c.next(), &ranges, 2);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
    }

    #[test]
    fn test_mixed() {
        let ranges = build_ranges("piipppipippip");
        let mut c = RangesIterator::new(ranges.clone());
        assert_it_pr(c.next(), &ranges, &[0]);
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_it_ir(c.next(), &ranges, 1);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_it_ir(c.next(), &ranges, 2);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_it_pr(c.next(), &ranges, &[3, 4, 5]);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_it_ir(c.next(), &ranges, 6);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_it_pr(c.next(), &ranges, &[7]);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_it_ir(c.next(), &ranges, 8);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_it_pr(c.next(), &ranges, &[9, 10]);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_it_ir(c.next(), &ranges, 11);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_it_pr(c.next(), &ranges, &[12]);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
    }
}
