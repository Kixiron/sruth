use abomonation_derive::Abomonation;
use differential_dataflow::difference::{Monoid, Semigroup};
use num_traits::Bounded;
use std::{
    cmp,
    fmt::Debug,
    ops::{Add, AddAssign, Mul},
};

/// A type for getting the maximum value of a stream
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Max<T> {
    pub value: T,
}

impl<T> Max<T> {
    pub const fn new(value: T) -> Self {
        Self { value }
    }
}

impl<T> Add<Self> for Max<T>
where
    T: Ord,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self {
            value: cmp::max(self.value, rhs.value),
        }
    }
}

impl<T> AddAssign<Self> for Max<T>
where
    for<'a> &'a T: Ord,
{
    fn add_assign(&mut self, rhs: Self) {
        if &self.value < &rhs.value {
            self.value = rhs.value;
        }
    }
}

impl<T> AddAssign<&Self> for Max<T>
where
    T: Clone,
    for<'a> &'a T: Ord,
{
    fn add_assign(&mut self, rhs: &Self) {
        if &self.value < &rhs.value {
            self.value = rhs.value.clone();
        }
    }
}

#[allow(clippy::suspicious_arithmetic_impl)]
impl<T> Mul<Self> for Max<T>
where
    T: Add<T, Output = T>,
{
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        Self {
            value: self.value + rhs.value,
        }
    }
}

impl<T> Mul<isize> for Max<T>
where
    T: Monoid + Bounded,
{
    type Output = Self;

    fn mul(self, rhs: isize) -> Self {
        if rhs < 1 {
            <Self as Monoid>::zero()
        } else {
            self
        }
    }
}

impl<T> Monoid for Max<T>
where
    T: Semigroup + Bounded,
{
    fn zero() -> Self {
        Self {
            value: T::min_value(),
        }
    }
}

impl<T> Semigroup for Max<T>
where
    T: Ord + Bounded + Clone + Debug + 'static,
{
    fn is_zero(&self) -> bool {
        self.value == T::min_value()
    }
}
