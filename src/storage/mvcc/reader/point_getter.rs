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

use kvproto::kvrpcpb::IsolationLevel;

use storage::mvcc::write::{Write, WriteType};
use storage::mvcc::{default_not_found_error, Lock, Result};
use storage::{Cursor, CursorBuilder, Key, Snapshot, Statistics, Value, CF_LOCK};
use storage::{CF_DEFAULT, CF_WRITE};

use super::util::CheckLockResult;

/// `PointGetter` factory.
pub struct PointGetterBuilder<S: Snapshot> {
    snapshot: S,
    multi: bool,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    ts: u64,
}

impl<S: Snapshot> PointGetterBuilder<S> {
    /// Initialize a new `PointGetterBuilder`.
    pub fn new(snapshot: S, ts: u64) -> Self {
        Self {
            snapshot,
            multi: true,
            fill_cache: true,
            omit_value: false,
            isolation_level: IsolationLevel::SI,
            ts,
        }
    }

    /// Set whether or not to get multiple keys.
    ///
    /// Defaults to `true`.
    #[inline]
    pub fn multi(mut self, multi: bool) -> Self {
        self.multi = multi;
        self
    }

    /// Set whether or not read operations should fill the cache.
    ///
    /// Defaults to `true`.
    #[inline]
    pub fn fill_cache(mut self, fill_cache: bool) -> Self {
        self.fill_cache = fill_cache;
        self
    }

    /// Set whether values of the user key should be omitted. When `omit_value` is `true`, the
    /// length of returned value will be 0.
    ///
    /// Previously this option is called `key_only`.
    ///
    /// Defaults to `false`.
    #[inline]
    pub fn omit_value(mut self, omit_value: bool) -> Self {
        self.omit_value = omit_value;
        self
    }

    /// Set the isolation level.
    ///
    /// Defaults to `IsolationLevel::SI`.
    #[inline]
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    /// Build `PointGetter` from the current configuration.
    pub fn build(self) -> Result<PointGetter<S>> {
        // If we only want to get single value, we can use prefix seek.
        let write_cursor = CursorBuilder::new(&self.snapshot, CF_WRITE)
            .fill_cache(self.fill_cache)
            .prefix_seek(!self.multi)
            .build()?;

        Ok(PointGetter {
            snapshot: self.snapshot,
            multi: self.multi,
            fill_cache: self.fill_cache,
            omit_value: self.omit_value,
            isolation_level: self.isolation_level,
            ts: self.ts,

            statistics: Statistics::default(),

            write_cursor,
            write_cursor_drained: true,
            lock_cursor: None,
            lock_cursor_drained: false,
            default_cursor: None,

            drained: false,
        })
    }
}

/// This struct can be used to get the value of user keys. Internally, rollbacks are ignored and
/// smaller version will be tried. If the isolation level is SI, locks will be checked first.
///
/// Use `PointGetterBuilder` to build `PointGetter`.
pub struct PointGetter<S: Snapshot> {
    snapshot: S,
    multi: bool,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    ts: u64,

    statistics: Statistics,

    write_cursor: Cursor<S::Iter>,
    write_cursor_drained: bool,
    /// Lock cursor and default cursor will be built only when necessary.
    lock_cursor: Option<Cursor<S::Iter>>,
    lock_cursor_drained: bool,
    default_cursor: Option<Cursor<S::Iter>>,

    drained: bool,
}

impl<S: Snapshot> PointGetter<S> {
    /// Take out and reset the statistics collected so far.
    #[inline]
    pub fn take_statistics(&mut self) -> Statistics {
        ::std::mem::replace(&mut self.statistics, Statistics::default())
    }

    /// Get the value of a user key.
    ///
    /// If `multi == false`, this function must be called only once. Future calls return nothing.
    /// If `multi == true`, keys must be given in non-descending order. Calls with smaller keys
    /// return nothing.
    pub fn get(&mut self, user_key: &Key) -> Result<Option<Value>> {
        if !self.multi {
            // Protect from calling `get()` multiple times when `multi == false`.
            if self.drained {
                return Ok(None);
            } else {
                self.drained = true;
            }
        }

        let mut ts = self.ts;

        match self.isolation_level {
            IsolationLevel::SI => {
                // Check for locks that signal concurrent writes in SI.
                match self.load_and_check_lock(user_key, ts)? {
                    CheckLockResult::NotLocked => {}
                    CheckLockResult::Locked(e) => return Err(e),
                    CheckLockResult::Ignored(new_ts) => ts = new_ts,
                }
            }
            IsolationLevel::RC => {}
        }

        self.load_data(user_key, ts)
    }

    /// Get a lock of a user key in the lock CF. If lock exists, it will be checked to
    /// see whether it conflicts with the given `ts`. If there is no conflict or no lock,
    /// the safe `ts` will be returned.
    #[inline]
    fn load_and_check_lock(&mut self, user_key: &Key, ts: u64) -> Result<CheckLockResult> {
        if self.multi {
            self.load_and_check_lock_multi_get(user_key, ts)
        } else {
            self.load_and_check_lock_single_get(user_key, ts)
        }
    }

    /// If only one `get()` will be called, we can use `snapshot.get_cf()` to get lock directly.
    fn load_and_check_lock_single_get(
        &mut self,
        user_key: &Key,
        ts: u64,
    ) -> Result<CheckLockResult> {
        self.statistics.lock.get += 1;
        let lock_value = self.snapshot.get_cf(CF_LOCK, user_key)?;

        if let Some(ref lock_value) = lock_value {
            self.statistics.lock.processed += 1;
            let lock = Lock::parse(lock_value)?;
            super::util::check_lock(user_key, ts, &lock)
        } else {
            Ok(CheckLockResult::NotLocked)
        }
    }

    /// If multiple `get()` will be called, we need to use cursor to read the lock.
    fn load_and_check_lock_multi_get(
        &mut self,
        user_key: &Key,
        ts: u64,
    ) -> Result<CheckLockResult> {
        self.ensure_lock_cursor()?;
        let lock_cursor = self.lock_cursor.as_mut().unwrap();
        if !self.lock_cursor_drained {
            return Ok(CheckLockResult::NotLocked);
        }
        if !lock_cursor.near_seek(user_key, &mut self.statistics.lock)? {
            // If we seek and get nothing, `lock_cursor` becomes invalid. So next time calling
            // `near_seek` will result in cursor jump back if the given key is smaller than the
            // current key. To keep cursor move in forward direction constantly, let's mark this
            // state. Additionally this protects us from https://github.com/tikv/tikv/issues/3378.
            self.lock_cursor_drained = false;
            return Ok(CheckLockResult::NotLocked);
        }
        if lock_cursor.key(&mut self.statistics.lock) == user_key.as_encoded().as_slice() {
            self.statistics.lock.processed += 1;
            let lock_value = lock_cursor.value(&mut self.statistics.lock);
            let lock = Lock::parse(lock_value)?;
            super::util::check_lock(user_key, ts, &lock)
        } else {
            Ok(CheckLockResult::NotLocked)
        }
    }

    /// Creates the lock cursor if not created. This function will only be called when
    /// `multi == true`.
    fn ensure_lock_cursor(&mut self) -> Result<()> {
        if self.lock_cursor.is_some() {
            return Ok(());
        }
        // Keys will be given in non-descending order, so forward mode cursor is fine.
        let cursor = CursorBuilder::new(&self.snapshot, CF_LOCK)
            .fill_cache(self.fill_cache)
            .build()?;
        self.lock_cursor = Some(cursor);
        self.lock_cursor_drained = true;
        Ok(())
    }

    /// Creates the default cursor if not created. This function will only be called when
    /// `multi == true`.
    fn ensure_default_cursor(&mut self) -> Result<()> {
        if self.default_cursor.is_some() {
            return Ok(());
        }
        let cursor = CursorBuilder::new(&self.snapshot, CF_DEFAULT)
            .fill_cache(self.fill_cache)
            .build()?;
        self.default_cursor = Some(cursor);
        Ok(())
    }

    /// Load the value.
    ///
    /// First, a correct version info in the Write CF will be sought. Then, value will be loaded
    /// from Default CF if necessary.
    fn load_data(&mut self, user_key: &Key, ts: u64) -> Result<Option<Value>> {
        if !self.write_cursor_drained {
            return Ok(None);
        }

        // Seek to `${user_key}_${ts}`.
        if !self
            .write_cursor
            .near_seek(&user_key.clone().append_ts(ts), &mut self.statistics.write)?
        {
            // If we seek to nothing, it means no write `key >= ${user_key}_${ts}`.
            // - If later we want to get a key >= current key, due to the above conclusion we can
            //   quit directly.
            // - If later we want to get a key < current key, we should prohibit this call.
            //   Returning nothing directly is safer than some undefined behaviour.
            // So in all scenarios we should not provide results in future calls when we enter this
            // branch.
            self.write_cursor_drained = false;
        }

        loop {
            if !self.write_cursor.valid()? {
                // Key space ended.
                return Ok(None);
            }
            // We may seek to another key. In this case, it means we cannot find the specified key.
            {
                let cursor_key = self.write_cursor.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(cursor_key, user_key.as_encoded().as_slice()) {
                    return Ok(None);
                }
            }

            self.statistics.write.processed += 1;
            let write = Write::parse(self.write_cursor.value(&mut self.statistics.write))?;

            match write.write_type {
                WriteType::Put => {
                    return Ok(Some(self.load_data_by_write(write, user_key)?));
                }
                WriteType::Delete => {
                    return Ok(None);
                }
                WriteType::Lock | WriteType::Rollback => {
                    // Continue iterate next `write`.
                }
            }

            self.write_cursor.next(&mut self.statistics.write);
        }
    }

    /// Load the value by the given `write`. If value is carried in `write`, it will be returned
    /// directly. Otherwise there will be a default CF look up.
    fn load_data_by_write(&mut self, write: Write, user_key: &Key) -> Result<Value> {
        if self.omit_value {
            return Ok(vec![]);
        }
        match write.short_value {
            Some(value) => {
                // Value is carried in `write`.
                Ok(value)
            }
            None => {
                if self.multi {
                    self.load_data_from_default_cf_multi_get(write, user_key)
                } else {
                    self.load_data_from_default_cf_single_get(write, user_key)
                }
            }
        }
    }

    /// Load the value from default CF. Use `snapshot.get_cf()` directly.
    fn load_data_from_default_cf_single_get(
        &mut self,
        write: Write,
        user_key: &Key,
    ) -> Result<Value> {
        // TODO: Not necessary to receive a `Write`.
        self.statistics.data.get += 1;
        let value = self
            .snapshot
            .get_cf(CF_DEFAULT, &user_key.clone().append_ts(write.start_ts))?;

        if let Some(value) = value {
            self.statistics.data.processed += 1;
            Ok(value)
        } else {
            Err(default_not_found_error(
                user_key.to_raw()?,
                write,
                "load_data_from_default_cf",
            ))
        }
    }

    /// Load the value from default CF. Use cursor to read value.
    fn load_data_from_default_cf_multi_get(
        &mut self,
        write: Write,
        user_key: &Key,
    ) -> Result<Value> {
        self.ensure_default_cursor()?;
        let value = super::util::near_load_data_by_write(
            &mut self.default_cursor.as_mut().unwrap(),
            user_key,
            write,
            &mut self.statistics,
        )?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::engine::{self, SEEK_BOUND, TEMP_DIR};
    use storage::mvcc::tests::*;
    use storage::{CFStatistics, Engine, Key, RocksEngine};
    use storage::{ALL_CFS, SHORT_VALUE_MAX_LEN};

    use kvproto::kvrpcpb::{Context, IsolationLevel};
    use raftstore::store::engine::SyncSnapshot;

    fn new_multi_point_getter<E: Engine>(engine: &E, ts: u64) -> PointGetter<E::Snap> {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        PointGetterBuilder::new(snapshot, ts)
            .isolation_level(IsolationLevel::SI)
            .build()
            .unwrap()
    }

    fn new_single_point_getter<E: Engine>(engine: &E, ts: u64) -> PointGetter<E::Snap> {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        PointGetterBuilder::new(snapshot, ts)
            .isolation_level(IsolationLevel::SI)
            .multi(false)
            .build()
            .unwrap()
    }

    fn must_get_key<S: Snapshot>(point_getter: &mut PointGetter<S>, key: &[u8]) {
        assert!(point_getter.get(&Key::from_raw(key)).unwrap().is_some());
    }

    fn must_get_value<S: Snapshot>(point_getter: &mut PointGetter<S>, key: &[u8], prefix: &[u8]) {
        let val = point_getter.get(&Key::from_raw(key)).unwrap().unwrap();
        assert!(val.starts_with(prefix));
    }

    fn must_get_none<S: Snapshot>(point_getter: &mut PointGetter<S>, key: &[u8]) {
        assert!(point_getter.get(&Key::from_raw(key)).unwrap().is_none());
    }

    fn must_get_err<S: Snapshot>(point_getter: &mut PointGetter<S>, key: &[u8]) {
        assert!(point_getter.get(&Key::from_raw(key)).is_err());
    }

    fn assert_seek_next_prev(stat: &CFStatistics, seek: usize, next: usize, prev: usize) {
        assert_eq!(
            stat.seek, seek,
            "expect seek to be {}, got {}",
            seek, stat.seek
        );
        assert_eq!(
            stat.next, next,
            "expect next to be {}, got {}",
            next, stat.next
        );
        assert_eq!(
            stat.prev, prev,
            "expect prev to be {}, got {}",
            prev, stat.prev
        );
    }

    /// Builds a sample engine with the following data:
    /// LOCK    bar                     (commit at 11)
    /// PUT     bar     -> barvvv...    (commit at 5)
    /// PUT     box     -> boxvv....    (commit at 9)
    /// DELETE  foo1                    (commit at 9)
    /// PUT     foo1    -> foo1vv...    (commit at 3)
    /// LOCK    foo2                    (commit at 101)
    /// ...
    /// LOCK    foo2                    (commit at 23)
    /// LOCK    foo2                    (commit at 21)
    /// PUT     foo2    -> foo2vv...    (commit at 5)
    /// DELETE  xxx                     (commit at 7)
    /// PUT     zz       -> zvzv....    (commit at 103)
    fn new_sample_engine() -> RocksEngine {
        let suffix = "v".repeat(SHORT_VALUE_MAX_LEN + 1);
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        must_prewrite_put(
            &engine,
            b"foo1",
            &format!("foo1{}", suffix).into_bytes(),
            b"foo1",
            2,
        );
        must_commit(&engine, b"foo1", 2, 3);
        must_prewrite_put(
            &engine,
            b"foo2",
            &format!("foo2{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
        must_prewrite_put(
            &engine,
            b"bar",
            &format!("bar{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
        must_commit(&engine, b"foo2", 4, 5);
        must_commit(&engine, b"bar", 4, 5);
        must_prewrite_delete(&engine, b"xxx", b"xxx", 6);
        must_commit(&engine, b"xxx", 6, 7);
        must_prewrite_put(
            &engine,
            b"box",
            &format!("box{}", suffix).into_bytes(),
            b"box",
            8,
        );
        must_prewrite_delete(&engine, b"foo1", b"box", 8);
        must_commit(&engine, b"box", 8, 9);
        must_commit(&engine, b"foo1", 8, 9);
        must_prewrite_lock(&engine, b"bar", b"bar", 10);
        must_commit(&engine, b"bar", 10, 11);
        for i in 20..100 {
            if i % 2 == 0 {
                must_prewrite_lock(&engine, b"foo2", b"foo2", i);
                must_commit(&engine, b"foo2", i, i + 1);
            }
        }
        must_prewrite_put(
            &engine,
            b"zz",
            &format!("zz{}", suffix).into_bytes(),
            b"zz",
            102,
        );
        must_commit(&engine, b"zz", 102, 103);
        engine
    }

    /// Builds a sample engine that contains transactions on the way and some short
    /// values embedded in the write CF. The data is as follows:
    /// DELETE  bar                     (start at 4)
    /// PUT     bar     -> barval       (commit at 3)
    /// PUT     foo1    -> foo1vv...    (commit at 3)
    /// PUT     foo2    -> foo2vv...    (start at 4)
    fn new_sample_engine_2() -> RocksEngine {
        let suffix = "v".repeat(SHORT_VALUE_MAX_LEN + 1);
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        must_prewrite_put(
            &engine,
            b"foo1",
            &format!("foo1{}", suffix).into_bytes(),
            b"foo1",
            2,
        );
        must_prewrite_put(&engine, b"bar", b"barval", b"foo1", 2);
        must_commit(&engine, b"foo1", 2, 3);
        must_commit(&engine, b"bar", 2, 3);

        must_prewrite_put(
            &engine,
            b"foo2",
            &format!("foo2{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
        must_prewrite_delete(&engine, b"bar", b"foo2", 4);
        engine
    }

    /// No ts larger than get ts
    #[test]
    fn test_multi_basic_1() {
        let engine = new_sample_engine();

        let mut getter = new_multi_point_getter(&engine, 200);

        // Get a deleted key
        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 1, 0, 0);
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);
        // Get again
        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 0, 0, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);

        // Get a key that exists
        must_get_value(&mut getter, b"foo2", b"foo2v");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        // We have to check every version so there is 42 next and 0 seek
        assert_seek_next_prev(&s.write, 0, 42, 0);
        assert_seek_next_prev(&s.data, 1, 0, 0);
        // Get again
        must_get_value(&mut getter, b"foo2", b"foo2v");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 0, 0, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);

        // Get a smaller key
        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 0, 0, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);

        // Get a key that does not exist
        must_get_none(&mut getter, b"z");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 0, 2, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);

        // Get a key that exists
        must_get_value(&mut getter, b"zz", b"zzv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 0, 0, 0);
        assert_seek_next_prev(&s.data, 0, 1, 0);
        // Get again
        must_get_value(&mut getter, b"zz", b"zzv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 0, 0, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);
    }

    /// Some ts larger than get ts
    #[test]
    fn test_multi_basic_2() {
        let engine = new_sample_engine();

        let mut getter = new_multi_point_getter(&engine, 5);

        must_get_value(&mut getter, b"bar", b"barv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 1, 0, 0);
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_seek_next_prev(&s.data, 1, 0, 0);

        must_get_value(&mut getter, b"bar", b"barv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 0, 0, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);

        must_get_none(&mut getter, b"bo");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 0, 1, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);

        must_get_none(&mut getter, b"box");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 0, 1, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);

        must_get_value(&mut getter, b"foo1", b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 0, 1, 0);
        assert_seek_next_prev(&s.data, 0, 2, 0);

        must_get_none(&mut getter, b"zz");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 1, SEEK_BOUND as usize, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);
    }

    /// All ts larger than get ts
    #[test]
    fn test_multi_basic_3() {
        let engine = new_sample_engine();

        let mut getter = new_multi_point_getter(&engine, 2);

        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 1, 0, 0);
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);

        must_get_none(&mut getter, b"non_exist");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 1, SEEK_BOUND as usize, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);

        // Cursor never move back.
        must_get_none(&mut getter, b"foo1");
        must_get_none(&mut getter, b"foo0");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 0, 0, 0);
        assert_seek_next_prev(&s.write, 0, 0, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);
    }

    /// There are some locks in the Lock CF.
    #[test]
    fn test_multi_locked() {
        let engine = new_sample_engine_2();

        let mut getter = new_multi_point_getter(&engine, 1);
        must_get_none(&mut getter, b"a");
        must_get_none(&mut getter, b"bar");
        must_get_none(&mut getter, b"foo1");
        must_get_none(&mut getter, b"foo2");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 1, 1, 0);
        assert_seek_next_prev(&s.write, 1, 2, 0);
        assert_seek_next_prev(&s.data, 0, 0, 0);

        let mut getter = new_multi_point_getter(&engine, 3);
        must_get_none(&mut getter, b"a");
        must_get_value(&mut getter, b"bar", b"barv");
        must_get_value(&mut getter, b"bar", b"barv");
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_none(&mut getter, b"foo2");
        must_get_none(&mut getter, b"foo2");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.lock, 1, 1, 0);
        assert_seek_next_prev(&s.write, 1, 2, 0);
        assert_seek_next_prev(&s.data, 1, 0, 0);

        let mut getter = new_multi_point_getter(&engine, 4);
        must_get_none(&mut getter, b"a");
        must_get_err(&mut getter, b"bar");
        must_get_err(&mut getter, b"bar");
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_err(&mut getter, b"foo2");
        must_get_none(&mut getter, b"zz");
        assert_seek_next_prev(&s.lock, 1, 1, 0);
        assert_seek_next_prev(&s.write, 1, 2, 0);
        assert_seek_next_prev(&s.data, 1, 0, 0);
    }

    /// Single Point Getter can only get once.
    #[test]
    fn test_single_basic() {
        let engine = new_sample_engine_2();

        let mut getter = new_single_point_getter(&engine, 1);
        must_get_none(&mut getter, b"foo1");

        let mut getter = new_single_point_getter(&engine, 3);
        must_get_value(&mut getter, b"bar", b"barv");
        must_get_none(&mut getter, b"bar");
        must_get_none(&mut getter, b"foo1");

        let mut getter = new_single_point_getter(&engine, 3);
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_none(&mut getter, b"foo2");

        let mut getter = new_single_point_getter(&engine, 3);
        must_get_none(&mut getter, b"foo2");
        must_get_none(&mut getter, b"foo2");

        let mut getter = new_single_point_getter(&engine, 4);
        must_get_err(&mut getter, b"bar");
        must_get_none(&mut getter, b"bar");
        must_get_none(&mut getter, b"a");
        must_get_none(&mut getter, b"foo1");

        let mut getter = new_single_point_getter(&engine, 4);
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_none(&mut getter, b"foo1");
    }

    #[test]
    fn test_omit_value() {
        let engine = new_sample_engine_2();

        let snapshot = engine.snapshot(&Context::new()).unwrap();

        let mut getter = PointGetterBuilder::new(snapshot.clone(), 4)
            .isolation_level(IsolationLevel::SI)
            .omit_value(true)
            .build()
            .unwrap();
        must_get_err(&mut getter, b"bar");
        must_get_key(&mut getter, b"foo1");
        must_get_err(&mut getter, b"foo2");
        must_get_none(&mut getter, b"foo3");

        fn new_omit_value_single_point_getter(
            snapshot: SyncSnapshot,
            ts: u64,
        ) -> PointGetter<SyncSnapshot> {
            PointGetterBuilder::new(snapshot, ts)
                .isolation_level(IsolationLevel::SI)
                .omit_value(true)
                .multi(false)
                .build()
                .unwrap()
        }

        let mut getter = new_omit_value_single_point_getter(snapshot.clone(), 4);
        must_get_err(&mut getter, b"bar");
        must_get_none(&mut getter, b"bar");

        let mut getter = new_omit_value_single_point_getter(snapshot.clone(), 4);
        must_get_key(&mut getter, b"foo1");
        must_get_none(&mut getter, b"foo1");

        let mut getter = new_omit_value_single_point_getter(snapshot.clone(), 4);
        must_get_none(&mut getter, b"foo3");
        must_get_none(&mut getter, b"foo3");
    }
}
