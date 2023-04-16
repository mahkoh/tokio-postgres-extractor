//! Extension traits for working with [`RowStream`]s.
//!
//! Using these extension traits, [`RowStream`]s can be turned into [`Stream`]s producing
//! [`Extract`][crate::Extract]able types.
//!
//! # Examples
//!
//! ```
//! # use futures_util::TryStreamExt;
//! # use tokio_postgres::{Error, Row, RowStream};
//! # use tokio_postgres_extractor::{Columns, Extract};
//! # use tokio_postgres_extractor::stream::RowStreamExtractExt;
//! #[derive(Columns, Extract)]
//! struct User {
//!     id: i32,
//!     name: String,
//! }
//!
//! async fn extract_users(i: RowStream) -> Result<Vec<User>, Error> {
//!     i.extract().try_collect().await
//! }
//! ```

use {
    crate::{stream::sealed::Sealed, ExtractOwned},
    futures_core::Stream,
    pin_project::pin_project,
    std::{
        pin::Pin,
        task::{Context, Poll},
    },
    tokio_postgres::{Error, RowStream},
};

#[cfg(test)]
mod tests;

/// A [`Stream`] producing `T`s from a [`RowStream`].
///
/// Construct it using [`RowStreamExtractExt::extract`].
///
/// # Panics
///
/// The stream panics if [`Extract::extract`][crate::Extract::extract] panics.
#[pin_project]
pub struct ExtractStream<T>
where
    T: ExtractOwned,
{
    /// The underlying stream.
    ///
    /// This field is public for easier access.
    #[pin]
    pub stream: RowStream,
    columns: Option<T::Columns>,
}

impl<T> Stream for ExtractStream<T>
where
    T: ExtractOwned,
{
    type Item = Result<T, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let slf = self.project();
        slf.stream
            .poll_next(cx)
            .map_ok(|row| T::extract(slf.columns, &row))
    }
}

/// A [`Stream`] producing `T`s from a [`Pin<&mut RowStream>`][RowStream].
///
/// Construct it using [`RowStreamExtractExt::extract_mut`].
///
/// # Panics
///
/// The stream panics if [`Extract::extract`][crate::Extract::extract] panics.
pub struct ExtractStreamMut<'a, T>
where
    T: ExtractOwned,
{
    /// The underlying stream.
    ///
    /// This field is public for easier access.
    pub stream: Pin<&'a mut RowStream>,
    columns: Option<T::Columns>,
}

impl<'a, T> Stream for ExtractStreamMut<'a, T>
where
    T: ExtractOwned,
{
    type Item = Result<T, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let slf = self.get_mut();
        slf.stream
            .as_mut()
            .poll_next(cx)
            .map_ok(|row| T::extract(&mut slf.columns, &row))
    }
}

/// Extension trait for extracting from a [`RowStream`].
pub trait RowStreamExtractExt: Sealed {
    /// Turns the [`RowStream`] into a [`Stream`] over `T`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use futures_util::TryStreamExt;
    /// # use tokio_postgres::{Error, Row, RowStream};
    /// # use tokio_postgres_extractor::{Columns, Extract};
    /// # use tokio_postgres_extractor::stream::RowStreamExtractExt;
    /// #[derive(Columns, Extract)]
    /// struct User {
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// async fn extract_users(i: RowStream) -> Result<Vec<User>, Error> {
    ///     i.extract().try_collect().await
    /// }
    /// ```
    fn extract<T: ExtractOwned>(self) -> ExtractStream<T>;

    ///
    /// # Examples
    ///
    /// ```
    /// # use std::pin::Pin;
    /// # use futures_util::TryStreamExt;
    /// # use tokio_postgres::{Error, Row, RowStream};
    /// # use tokio_postgres_extractor::{Columns, Extract};
    /// # use tokio_postgres_extractor::stream::RowStreamExtractExt;
    /// #[derive(Columns, Extract)]
    /// struct User {
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// async fn extract_users(i: Pin<&mut RowStream>) -> Result<Vec<User>, Error> {
    ///     i.extract_mut().try_collect().await
    /// }
    /// ```
    fn extract_mut<T: ExtractOwned>(self: Pin<&mut Self>) -> ExtractStreamMut<'_, T>;
}

impl RowStreamExtractExt for RowStream {
    fn extract<T: ExtractOwned>(self) -> ExtractStream<T> {
        ExtractStream {
            stream: self,
            columns: None,
        }
    }

    fn extract_mut<T: ExtractOwned>(self: Pin<&mut Self>) -> ExtractStreamMut<'_, T> {
        ExtractStreamMut {
            stream: self,
            columns: None,
        }
    }
}

impl Sealed for RowStream {}

mod sealed {
    pub trait Sealed {}
}
