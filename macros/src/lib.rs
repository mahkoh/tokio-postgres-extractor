#![allow(clippy::len_zero)]

use {
    crate::{column::columns_impl, extract::extract_impl},
    proc_macro::TokenStream,
    syn::{parse_macro_input, DeriveInput},
};

mod column;
mod extract;

#[proc_macro_derive(Columns, attributes(column))]
pub fn columns(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    columns_impl(input)
        .unwrap_or_else(|e| e.into_compile_error())
        .into()
}

#[proc_macro_derive(Extract)]
pub fn extract(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    extract_impl(input)
        .unwrap_or_else(|e| e.into_compile_error())
        .into()
}
