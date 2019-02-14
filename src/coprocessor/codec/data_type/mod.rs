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

// FIXME: Move to cop_datatype. Currently it refers some types in `crate::coprocessor::codec::mysql`
// so that it is not possible to move.

mod scalar;
mod vector;

// Concrete eval types without a nullable wrapper.
pub type Int = i64;
pub type Real = f64;
pub type Bytes = Vec<u8>;
pub use crate::coprocessor::codec::mysql::{Decimal, Duration, Json, Time as DateTime};

// Dynamic eval types.
pub use self::scalar::ScalarValue;
pub use self::vector::VectorValue;

/// A trait of evaluating current concrete eval type into a MySQL logic value, represented by
/// Rust's `bool` type.
pub trait AsMySQLBool {
    /// Evaluates into a MySQL logic value.
    fn as_mysql_bool(&self) -> bool;
}

impl AsMySQLBool for Int {
    #[inline]
    fn as_mysql_bool(&self) -> bool {
        *self != 0
    }
}

impl AsMySQLBool for Real {
    #[inline]
    fn as_mysql_bool(&self) -> bool {
        self.round() != 0f64
    }
}

impl AsMySQLBool for Bytes {
    #[inline]
    fn as_mysql_bool(&self) -> bool {
        // FIXME: No unwrap?? No without_context??
        !self.is_empty()
            && crate::coprocessor::codec::convert::bytes_to_int_without_context(self).unwrap() != 0
    }
}

impl<T> AsMySQLBool for Option<T>
where
    T: AsMySQLBool,
{
    fn as_mysql_bool(&self) -> bool {
        match self {
            None => false,
            Some(ref v) => v.as_mysql_bool(),
        }
    }
}

/// A trait of all types that can be used during evaluation (eval type).
pub trait Evaluable: Clone {
    /// Borrows this concrete type from a `ScalarValue` in the same type.
    fn borrow_scalar_value(v: &ScalarValue) -> &Self;

    /// Borrows a slice of this concrete type from a `VectorValue` in the same type.
    fn borrow_vector_value(v: &VectorValue) -> &[Self];

    /// Converts a vector of this concrete type into a `VectorValue` in the same type.
    fn into_vector_value(vec: Vec<Self>) -> VectorValue;
}

macro_rules! impl_evaluable_type {
    ($ty:ty) => {
        impl Evaluable for $ty {
            #[inline]
            fn borrow_scalar_value(v: &ScalarValue) -> &Self {
                v.as_ref()
            }

            #[inline]
            fn borrow_vector_value(v: &VectorValue) -> &[Self] {
                v.as_ref()
            }

            #[inline]
            fn into_vector_value(vec: Vec<Self>) -> VectorValue {
                VectorValue::from(vec)
            }
        }
    };
}

impl_evaluable_type! { Option<Int> }
impl_evaluable_type! { Option<Real> }
impl_evaluable_type! { Option<Decimal> }
impl_evaluable_type! { Option<Bytes> }
impl_evaluable_type! { Option<DateTime> }
impl_evaluable_type! { Option<Duration> }
impl_evaluable_type! { Option<Json> }
