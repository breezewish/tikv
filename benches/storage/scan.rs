use test::Bencher;
use tempdir::TempDir;
use super::super::util::KvGenerator;

use tikv::storage::{Mutation, Key, SHORT_VALUE_MAX_LEN};
use kvproto::kvrpcpb::Context;

fn bench_scan(b: &mut Bencher, forward_scan: bool, versions: usize, long_value: bool) {
    const NUMBER_KEYS: usize = 10000;

    let path = TempDir::new("").unwrap();
    let store = super::util::new_storage(path.path().to_str().unwrap());

    println!("Preparing data...");

    let value_length = if long_value {
        SHORT_VALUE_MAX_LEN + 1
    } else {
        5
    };
    let kvs: Vec<_> = KvGenerator::new_by_seed(0xFEE1DEAD, 32, value_length)
        .take(NUMBER_KEYS)
        .collect();
    let mut ts_generator = 1..;
    for _ in 0..versions {
        let ts = ts_generator.next().unwrap();
        let mutations: Vec<_> = kvs.iter()
            .map(|(k, v)| Mutation::Put((Key::from_raw(k), v.clone())))
            .collect();
        store
            .prewrite(Context::new(), mutations, kvs[0].0.clone(), ts)
            .unwrap();
        let keys: Vec<_> = kvs.iter().map(|(k, _)| Key::from_raw(k)).collect();
        store
            .commit(Context::new(), keys, ts, ts_generator.next().unwrap())
            .unwrap();
    }

    println!("Prepare complete, benchmarking...");

    let ts = ts_generator.next().unwrap();

    if forward_scan {
        b.iter(|| {
            let kvs = store
                .scan(
                    super::util::new_no_cache_context(),
                    test::black_box(Key::from_raw(&[])),
                    NUMBER_KEYS + 1,
                    false,
                    ts,
                )
                .unwrap();
            assert_eq!(kvs.len(), NUMBER_KEYS);
        });
    } else {
        let start_key = Key::from_raw(&[0xFF].repeat(200));
        b.iter(|| {
            let kvs = store
                .reverse_scan(
                    super::util::new_no_cache_context(),
                    test::black_box(start_key.clone()),
                    NUMBER_KEYS + 1,
                    false,
                    ts,
                )
                .unwrap();
            assert_eq!(kvs.len(), NUMBER_KEYS);
        });
    }
}

#[bench]
fn bench_forward_scan_1_version(b: &mut Bencher) {
    bench_scan(b, true, 1, false);
}

#[bench]
fn bench_forward_scan_1_version_long_value(b: &mut Bencher) {
    bench_scan(b, true, 1, true);
}

#[bench]
fn bench_forward_scan_2_versions(b: &mut Bencher) {
    bench_scan(b, true, 2, false);
}

#[bench]
fn bench_forward_scan_2_versions_long_value(b: &mut Bencher) {
    bench_scan(b, true, 2, true);
}

#[bench]
fn bench_forward_scan_5_versions(b: &mut Bencher) {
    bench_scan(b, true, 5, false);
}

#[bench]
fn bench_forward_scan_10_versions(b: &mut Bencher) {
    bench_scan(b, true, 10, false);
}

#[bench]
fn bench_forward_scan_50_versions(b: &mut Bencher) {
    bench_scan(b, true, 50, false);
}

#[bench]
fn bench_backward_scan_1_version(b: &mut Bencher) {
    bench_scan(b, false, 1, false);
}

#[bench]
fn bench_backward_scan_1_version_long_value(b: &mut Bencher) {
    bench_scan(b, false, 1, true);
}

#[bench]
fn bench_backward_scan_2_versions(b: &mut Bencher) {
    bench_scan(b, false, 2, false);
}

#[bench]
fn bench_backward_scan_2_versions_long_value(b: &mut Bencher) {
    bench_scan(b, false, 2, true);
}

#[bench]
fn bench_backward_scan_5_versions(b: &mut Bencher) {
    bench_scan(b, false, 5, false);
}

#[bench]
fn bench_backward_scan_10_versions(b: &mut Bencher) {
    bench_scan(b, false, 10, false);
}

#[bench]
fn bench_backward_scan_50_versions(b: &mut Bencher) {
    bench_scan(b, false, 50, false);
}
