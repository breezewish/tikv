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

pub mod util;

use cop_datatype::FieldTypeTp;

/// COUNT(1)
fn bench_simple_aggr_count_1(b: &mut criterion::Bencher, input: &Input) {
    let expr = crate::util::aggr::create_expr_count_1();
    input.bencher.bench(b, &expr, input.src_rows);
}

/// COUNT(COL) where COL is a int column
fn bench_simple_aggr_count_int_column(b: &mut criterion::Bencher, input: &Input) {
    let expr = crate::util::aggr::create_expr_count_column(0, FieldTypeTp::LongLong);
    input.bencher.bench(b, &expr, input.src_rows);
}

/// COUNT(COL) where COL is a real column
fn bench_simple_aggr_count_real_column(b: &mut criterion::Bencher, input: &Input) {
    let expr = crate::util::aggr::create_expr_count_column(1, FieldTypeTp::Double);
    input.bencher.bench(b, &expr, input.src_rows);
}

/// COUNT(COL) where COL is a bytes column (note: the column is very short)
fn bench_simple_aggr_count_bytes_column(b: &mut criterion::Bencher, input: &Input) {
    let expr = crate::util::aggr::create_expr_count_column(2, FieldTypeTp::VarChar);
    input.bencher.bench(b, &expr, input.src_rows);
}

struct Input {
    /// How many rows to aggregate
    src_rows: usize,

    /// The aggregate executor (batch / normal) to use
    bencher: Box<dyn util::SimpleAggrBencher>,
}

impl Clone for Input {
    fn clone(&self) -> Self {
        Input {
            src_rows: self.src_rows,
            bencher: self.bencher.box_clone(),
        }
    }
}

impl std::fmt::Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/rows={}", self.bencher.name(), self.src_rows)
    }
}

pub fn bench(c: &mut criterion::Criterion) {
    let mut inputs = vec![];
    let src_rows_options = vec![1, 10, 1024, 5000];
    let bencher_options: Vec<Box<dyn util::SimpleAggrBencher>> = vec![
        Box::new(util::NormalSimpleAggrExecutorBencher),
        Box::new(util::BatchSimpleAggrExecutorBencher),
    ];

    for bencher in &bencher_options {
        for src_rows in &src_rows_options {
            inputs.push(Input {
                src_rows: *src_rows,
                bencher: bencher.box_clone(),
            });
        }
    }

    c.bench_function_over_inputs(
        "simple_aggr_count_1",
        bench_simple_aggr_count_1,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "simple_aggr_count_int_column",
        bench_simple_aggr_count_int_column,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "simple_aggr_count_real_column",
        bench_simple_aggr_count_real_column,
        inputs.clone(),
    );
    c.bench_function_over_inputs(
        "simple_aggr_count_bytes_column",
        bench_simple_aggr_count_bytes_column,
        inputs.clone(),
    );
}
