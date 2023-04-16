//! Extension traits for working with iterators over [`Row`]s.
//!
//! Using these extension traits, iterators over [`Row`]s can be turned into iterators
//! over [`Extract`]able types.
//!
//! # Examples
//!
//! ```
//! # use tokio_postgres::Row;
//! # use tokio_postgres_extractor::{Columns, Extract};
//! # use tokio_postgres_extractor::iter::IterExtractRefExt;
//! #[derive(Columns, Extract)]
//! struct User<'a> {
//!     id: i32,
//!     name: &'a str,
//! }
//!
//! fn extract_users<'a>(i: impl Iterator<Item = &'a Row>) -> Vec<User<'a>> {
//!     i.extract_ref().collect()
//! }
//! ```

use {
    crate::{
        iter::sealed::{Sealed1, Sealed2},
        Extract, ExtractOwned,
    },
    tokio_postgres::Row,
};

#[cfg(test)]
mod tests;

/// An iterator over `T`s that are extracted from [`Row`]s.
///
/// Construct it using [`IterExtractExt::extract`].
///
/// # Panics
///
/// The iterator panics if [`Extract::extract`] panics.
pub struct ExtractIter<T, I>
where
    T: ExtractOwned,
    I: Iterator<Item = Row>,
{
    iter: I,
    columns: Option<T::Columns>,
}

impl<T, I> Iterator for ExtractIter<T, I>
where
    T: ExtractOwned,
    I: Iterator<Item = Row>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        Some(T::extract(&mut self.columns, &self.iter.next()?))
    }
}

/// Extension trait for extracting from an iterator over [`Row`].
pub trait IterExtractExt: Iterator<Item = Row> + Sized + Sealed2 {
    /// Turns the iterator into an iterator over `T`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use tokio_postgres::Row;
    /// # use tokio_postgres_extractor::iter::IterExtractExt;
    /// # use tokio_postgres_extractor::{Columns, Extract};
    /// #[derive(Columns, Extract)]
    /// struct User {
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// fn extract_users(i: impl Iterator<Item = Row>) -> Vec<User> {
    ///     i.extract().collect()
    /// }
    fn extract<T: ExtractOwned>(self) -> ExtractIter<T, Self>;
}

impl<I> Sealed2 for I where I: Iterator<Item = Row> {}

impl<I> IterExtractExt for I
where
    I: Iterator<Item = Row>,
{
    fn extract<T: ExtractOwned>(self) -> ExtractIter<T, Self> {
        ExtractIter {
            iter: self,
            columns: None,
        }
    }
}

/// An iterator over `T`s that are extracted from [`&Row`][Row]s.
///
/// Construct it using [`IterExtractRefExt::extract_ref`].
///
/// # Panics
///
/// The iterator panics if [`Extract::extract`] panics.
pub struct ExtractIterRef<'a, T, I>
where
    T: Extract<'a>,
    I: Iterator<Item = &'a Row>,
{
    iter: I,
    columns: Option<T::Columns>,
}

impl<'a, T, I> Iterator for ExtractIterRef<'a, T, I>
where
    T: Extract<'a>,
    I: Iterator<Item = &'a Row>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        Some(T::extract(&mut self.columns, self.iter.next()?))
    }
}

/// Extension trait for extracting from an iterator over [`&Row`][Row].
pub trait IterExtractRefExt<'a>: Iterator<Item = &'a Row> + Sized + Sealed1 {
    /// Turns the iterator into an iterator over `T`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use tokio_postgres::Row;
    /// # use tokio_postgres_extractor::iter::IterExtractRefExt;
    /// # use tokio_postgres_extractor::{Columns, Extract};
    /// #[derive(Columns, Extract)]
    /// struct User<'a> {
    ///     id: i32,
    ///     name: &'a str,
    /// }
    ///
    /// fn extract_users<'a>(i: impl Iterator<Item = &'a Row>) -> Vec<User<'a>> {
    ///     i.extract_ref().collect()
    /// }
    /// ```
    fn extract_ref<T: Extract<'a>>(self) -> ExtractIterRef<'a, T, Self>;
}

impl<'a, I> Sealed1 for I where I: Iterator<Item = &'a Row> {}

impl<'a, I> IterExtractRefExt<'a> for I
where
    I: Iterator<Item = &'a Row>,
{
    fn extract_ref<T: Extract<'a>>(self) -> ExtractIterRef<'a, T, Self> {
        ExtractIterRef {
            iter: self,
            columns: None,
        }
    }
}

mod sealed {
    pub trait Sealed1 {}
    pub trait Sealed2 {}
}
