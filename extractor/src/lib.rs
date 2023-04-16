//! High-performance extractors for [tokio_postgres].
//!
//! This crate contains traits and proc macros for creating high-performance extractors
//! for Rust types from [`Row`]s.
//!
//! # Examples
//!
//! ## Extracting a single row
//!
//! ```
//! # use tokio_postgres::{Client, Error};
//! # use tokio_postgres_extractor::RowExtractExt;
//! # use tokio_postgres_extractor::Columns;
//! # use tokio_postgres_extractor::Extract;
//! #[derive(Columns, Extract)]
//! struct User {
//!     id: i32,
//!     name: String,
//! }
//!
//! async fn get_user(client: &Client, id: i32) -> Result<User, Error> {
//!     client
//!         .query_one("select * from user where id = $1", &[&id])
//!         .await
//!         .map(|r| r.extract_once())
//! }
//! ```
//!
//! ## Extracting a stream of rows
//!
//! ```
//! # use futures_util::TryStreamExt;
//! # use tokio_postgres::{Client, Error};
//! # use tokio_postgres_extractor::stream::RowStreamExtractExt;
//! # use tokio_postgres_extractor::Columns;
//! # use tokio_postgres_extractor::Extract;
//! #[derive(Columns, Extract)]
//! struct User {
//!     id: i32,
//!     name: String,
//! }
//!
//! async fn get_users(client: &Client) -> Result<Vec<User>, Error> {
//!     client
//!         .query_raw("select * from user", None::<i32>)
//!         .await?
//!         .extract()
//!         .try_collect()
//!         .await
//! }
//! ```
//!
//! # Generic types
//!
//! Generic types are fully supported.
//!
//! ```
//! # use tokio_postgres_extractor::Columns;
//! # use tokio_postgres_extractor::Extract;
//! #[derive(Columns, Extract)]
//! struct User<'a, T: ?Sized> {
//!     id: i32,
//!     name: &'a T,
//! }
//!
//! fn assert_is_extract<'a, T: Extract<'a>>() { }
//!
//! assert_is_extract::<User<str>>();
//! ```
//!
//! # Custom column names
//!
//! You can specify column names that are different from field names. See the
//! documentation of the [`Columns`][macro@Columns] proc macro.
//!
//! # Design
//!
//! A naive mapping function such as
//!
//! ```rust,ignore
//! User {
//!     id: row.get("id"),
//!     name: row.get("name"),
//! }
//! ```
//!
//! has `O(N^2)` runtime where `N` is the number of columns. Each invocation of `row.get`
//! must walk all columns and compare their name to the name of the requested column. This
//! crate solves this by
//!
//! 1. Constructing an efficient data structure at compile time that reduces lookup time
//!    to `O(N)`.
//!
//!    This data structure is similar to a perfect hash function but more efficient.
//!
//! 2. Memorizing the mapping from fields to columns whenever possible.

extern crate self as tokio_postgres_extractor;

/// Proc macro for deriving the [`Columns`] trait.
///
/// # Custom column names
///
/// If the column name is different from the name of the field, you can use
///
/// ```rust,ignore
/// #[column(name = "Type")]`
/// ty: i32,
/// ```
///
/// to explicitly specify a name. The name must be a string literal.
///
/// # Explicit indices
///
/// If you already know the index a field maps to, you can use
///
/// ```rust,ignore
/// #[column(idx = 123)]`
/// ty: i32,
/// ```
///
/// to specify it.
///
/// # Implementation
///
/// The critical section in the expansion of
///
/// ```
/// # use tokio_postgres_extractor_macros::Columns;
/// #[derive(Columns)]
/// struct Account {
///     account_id: i32,
///     account_name: String,
///     account_role: i64,
/// }
/// ```
///
/// is
///
/// ```rust,ignore
/// for column in row.columns().iter() {
///     let name = column.name();
///     let idx = match name.len() {
///         10 => match name {
///             "account_id" => 0,
///             _ => continue,
///         },
///         12 => {
///             let b = name.as_bytes();
///             let disc = b[8];
///             match disc {
///                 110 => match name {
///                     "account_name" => 1,
///                     _ => continue,
///                 },
///                 114 => match name {
///                     "account_role" => 2,
///                     _ => continue,
///                 },
///                 _ => continue,
///             }
///         }
///         _ => continue,
///     };
///     // ...
/// }
/// ```
///
/// meaning that for each column the code
///
/// 1. uses a jump table indexed with the length of the column name,
/// 2. extracts an integer of size 1, 2, 4, or 8 from the column name,
/// 3. compares this integer to a number of candidates,
/// 4. compares the column name to the candidate.
///
/// This is very fast on current hardware. If the name of the candidate is no more than 16
/// bytes, then the final comparison will compile to a number of assembly instructions on
/// x86_64. Otherwise it compiles to a call to `bcmp`. If you compile with `-C
/// target-cpu=native`, then even these calls to `bcmp` will be inlined.
///
/// In practice, this means that the implementation of `Columns` is usually `O(N)` in the
/// number of columns with a "small" constant. In some degenerate cases the construction
/// above is not possible and the implementation will have to perform multiple string
/// comparisons. Even this is still much faster than using the phf crate or similar.
pub use tokio_postgres_extractor_macros::Columns;
/// Proc macro for deriving the [`Extract`] trait.
pub use tokio_postgres_extractor_macros::Extract;
use {crate::sealed::Sealed, std::ops::Index, tokio_postgres::Row};

pub mod iter;
pub mod stream;

#[cfg(test)]
mod tests;

#[doc(hidden)]
pub mod private {
    pub use tokio_postgres;
}

/// A type whose fields map to Postgres columns.
///
/// This trait is almost always derived with the [Columns](macro@Columns) proc macro:
///
/// ```
/// # use tokio_postgres_extractor::Columns;
/// #[derive(Columns)]
/// struct User<'a> {
///     id: i32,
///     name: &'a str,
/// }
/// ```
///
/// In this case the associated `Columns` type is `[usize; N]` where `N` is the number of
/// fields.
///
/// Assume that a [`Row`] was created from the following query:
///
/// ```sql
/// select 'john' user, 123 balance, 1 id;
/// ```
///
/// Then the implementation of `Columns` for the type `User` above should return `[2, 0]`
/// because the `id` field maps to the third column and the `name` field maps to the
/// first column.
pub trait Columns {
    /// The type identifying the columns.
    ///
    /// This should always be `[usize; N]` where `N` is the number of fields. In a future
    /// version of this crate, this field will be replaced by
    ///
    /// ```
    /// pub trait Columns {
    ///     const NUM_COLUMNS: usize;
    /// }
    /// ```
    type Columns: Unpin + Index<usize, Output = usize>;

    /// Returns the mapping from the type's fields to the columns in a [`Row`].
    fn columns(row: &Row) -> Self::Columns;
}

/// A type that can be extracted from a [`Row`].
///
/// This trait is usually derived:
///
/// ```
/// # use tokio_postgres_extractor_macros::{Columns, Extract};
/// #[derive(Columns, Extract)]
/// struct User<'a> {
///     id: i32,
///     name: &'a str,
/// }
/// ```
///
/// Most of the time you will not use this trait directly but one of the utility functions:
///
/// - [`IterExtractExt::extract`][iter::IterExtractExt::extract]
/// - [`IterExtractRefExt::extract_ref`][iter::IterExtractRefExt::extract_ref]
/// - [`RowStreamExtractExt::extract`][stream::RowStreamExtractExt::extract]
/// - [`RowStreamExtractExt::extract_mut`][stream::RowStreamExtractExt::extract_mut]
///
/// # Examples
///
/// ```
/// # use tokio_postgres::{Client, Error};
/// # use tokio_postgres_extractor::{Columns, Extract};
/// #[derive(Columns, Extract)]
/// struct User {
///     id: i32,
///     name: String,
/// }
///
/// async fn get_user(client: &Client, id: u32) -> Result<User, Error> {
///     client
///         .query_one("select * from user where id = $1", &[&id])
///         .await
///         .map(|r| User::extract_once(&r))
/// }
/// ```
pub trait Extract<'row>: Columns + Sized {
    /// Extracts an instance of the type from a [`Row`].
    ///
    /// If the implementation of [`Columns`] was derived, then this function is almost
    /// always faster than manually calling [`Row::get`] with the column names. Usually it
    /// is much faster.
    ///
    /// However, if you are extracting more than one row at at time, then you will want to
    /// instead call
    ///
    /// - [`Extract::extract`],
    /// - [`IterExtractExt::extract`][iter::IterExtractExt::extract],
    /// - [`IterExtractRefExt::extract_ref`][iter::IterExtractRefExt::extract_ref],
    /// - [`RowStreamExtractExt::extract`][stream::RowStreamExtractExt::extract], or
    /// - [`RowStreamExtractExt::extract_mut`][stream::RowStreamExtractExt::extract_mut].
    ///
    /// These function memorize the output of [`Columns::columns`] and are often 2x as fast
    /// per row as this function.
    ///
    /// # Panics
    ///
    /// Panics if [`Columns::columns`] or [`Row::get`] panics.
    ///
    /// # Examples
    ///
    /// ```
    /// # use tokio_postgres::Row;
    /// # use tokio_postgres_extractor::{Columns, Extract};
    /// #[derive(Columns, Extract)]
    /// struct User<'a> {
    ///     id: i32,
    ///     name: &'a str,
    /// }
    ///
    /// fn map_user(row: &Row) -> User<'_> {
    ///     User::extract_once(row)
    /// }
    /// ```
    fn extract_once(row: &'row Row) -> Self {
        Self::extract(&mut None, row)
    }

    /// Extracts an instance of the type from a [`Row`], memorizing the mapping between
    /// fields and columns.
    ///
    /// If you call this function multiple times with a memorized mapping, then you should
    /// make sure that the columns in the rows are in the same order. This is always the
    /// case if the rows were produced by a single SQL statement. Otherwise this function
    /// might panic or the mapping might be incorrect.
    ///
    /// Often you will want to call
    ///
    /// - [`IterExtractExt::extract`][iter::IterExtractExt::extract],
    /// - [`IterExtractRefExt::extract_ref`][iter::IterExtractRefExt::extract_ref],
    /// - [`RowStreamExtractExt::extract`][stream::RowStreamExtractExt::extract], or
    /// - [`RowStreamExtractExt::extract_mut`][stream::RowStreamExtractExt::extract_mut].
    ///
    /// instead, which hide the memorization behind and abstraction.
    ///
    /// # Panics
    ///
    /// Panics if [`Columns::columns`] or [`Row::get`] panics.
    ///
    /// # Examples
    ///
    /// ```
    /// # use futures_util::StreamExt;
    /// # use tokio_postgres::Row;
    /// # use tokio_postgres_extractor::{Columns, Extract};
    /// #[derive(Columns, Extract)]
    /// struct User<'a> {
    ///     id: i32,
    ///     name: &'a str,
    /// }
    ///
    /// fn map_users(rows: &[Row]) -> Vec<User<'_>> {
    ///     let mut columns = None;
    ///     rows.iter().map(|row| User::extract(&mut columns, row)).collect()
    /// }
    /// ```
    fn extract(columns: &mut Option<<Self as Columns>::Columns>, row: &'row Row) -> Self {
        Self::extract_with_columns(
            columns.get_or_insert_with(|| <Self as Columns>::columns(row)),
            row,
        )
    }

    /// Extracts an instance of the type from a [`Row`] and a mapping between the
    /// fields and columns.
    ///
    /// This function is usually derived with the [`Extract`](macro@Extract) proc macro.
    /// In this case the implementation looks as follows:
    ///
    /// ```
    /// # use futures_util::StreamExt;
    /// # use tokio_postgres::Row;
    /// # use tokio_postgres::types::FromSql;
    /// # use tokio_postgres_extractor::{Columns, Extract};
    /// #[derive(Columns)]
    /// struct User<'a> {
    ///     id: i32,
    ///     name: &'a str,
    /// }
    ///
    /// impl<'a, 'row> Extract<'row> for User<'a>
    /// where
    ///     i32: FromSql<'row>,
    ///     &'a str: FromSql<'row>,
    /// {
    ///     fn extract_with_columns(columns: &Self::Columns, row: &'row Row) -> Self {
    ///         Self {
    ///             id: row.get(columns[0]),
    ///             name: row.get(columns[1]),
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if [`Row::get`] panics.
    fn extract_with_columns(columns: &<Self as Columns>::Columns, row: &'row Row) -> Self;
}

/// A type that can be extracted from a [`Row`] without borrowing the [`Row`].
///
/// This is primarily useful for trait bounds on functions. For example
///
/// ```
/// # use tokio_postgres::Row;
/// # use tokio_postgres_extractor::ExtractOwned;
/// # fn get_row() -> Row {
/// #     unimplemented!()
/// # }
/// fn extract<T: ExtractOwned>() -> T {
///     let row: Row = get_row();
///     T::extract_once(&row)
/// }
/// ```
pub trait ExtractOwned: for<'a> Extract<'a> {}

impl<T> ExtractOwned for T where T: for<'a> Extract<'a> {}

/// Extension trait for extracting from a [`Row`].
pub trait RowExtractExt: Sealed {
    /// Extracts an instance of `T` from this [`Row`].
    ///
    /// This is equivalent to [`T::extract_once(self)`][Extract::extract_once].
    ///
    /// # Panics
    ///
    /// Panics if [`Extract::extract_once`] panics.
    ///
    /// # Examples
    ///
    /// ```
    /// # use tokio_postgres::Row;
    /// # use tokio_postgres_extractor::RowExtractExt;
    /// # use tokio_postgres_extractor_macros::{Columns, Extract};
    /// #[derive(Columns, Extract)]
    /// struct User<'a> {
    ///     id: i32,
    ///     name: &'a str,
    /// }
    ///
    /// fn map_user(row: &Row) -> User<'_> {
    ///     row.extract_once()
    /// }
    /// ```
    fn extract_once<'row, T>(&'row self) -> T
    where
        T: Extract<'row>;

    /// Extracts an instance of `T` from this [`Row`], memorizing the mapping between
    /// fields and columns.
    ///
    /// This is equivalent to [`T::extract(columns, self)`][Extract::extract].
    ///
    /// # Panics
    ///
    /// Panics if [`Extract::extract`] panics.
    ///
    /// # Examples
    ///
    /// ```
    /// # use futures_util::StreamExt;
    /// # use tokio_postgres::Row;
    /// # use tokio_postgres_extractor::{Columns, Extract, RowExtractExt};
    /// #[derive(Columns, Extract)]
    /// struct User<'a> {
    ///     id: i32,
    ///     name: &'a str,
    /// }
    ///
    /// fn map_users(rows: &[Row]) -> Vec<User<'_>> {
    ///     let mut columns = None;
    ///     rows.iter().map(|row| row.extract(&mut columns)).collect()
    /// }
    /// ```
    fn extract<'row, T>(&'row self, columns: &mut Option<<T as Columns>::Columns>) -> T
    where
        T: Extract<'row>;

    /// Extracts an instance of `T` from this [`Row`] and a mapping between the
    /// fields and columns.
    ///
    /// This is equivalent to [`T::extract_with_columns(columns, self)`][Extract::extract_with_columns].
    ///
    /// # Panics
    ///
    /// Panics if [`Extract::extract_with_columns`] panics.
    fn extract_with_columns<'row, T>(&'row self, columns: &<T as Columns>::Columns) -> T
    where
        T: Extract<'row>;
}

impl Sealed for Row {}

impl RowExtractExt for Row {
    fn extract_once<'row, T: Extract<'row>>(&'row self) -> T {
        T::extract_once(self)
    }

    fn extract<'row, T>(&'row self, columns: &mut Option<<T as Columns>::Columns>) -> T
    where
        T: Extract<'row>,
    {
        T::extract(columns, self)
    }
    fn extract_with_columns<'row, T>(&'row self, columns: &<T as Columns>::Columns) -> T
    where
        T: Extract<'row>,
    {
        T::extract_with_columns(columns, self)
    }
}

mod sealed {
    pub trait Sealed {}
}
