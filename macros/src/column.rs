use {
    proc_macro2::{Literal, Span, TokenStream},
    quote::quote,
    std::collections::{hash_map::Entry, HashMap, HashSet},
    syn::{
        parse::Parser, parse_quote_spanned, punctuated::Punctuated, spanned::Spanned, Attribute,
        Data, DeriveInput, Error, Expr, Field, Fields, Lit, LitStr, Meta, Path, Token,
    },
};

pub fn columns_impl(input: DeriveInput) -> Result<TokenStream, Error> {
    let str = match input.data {
        Data::Struct(s) => s,
        _ => {
            return Err(Error::new_spanned(
                input,
                "`Columns` can only be derive for structs",
            ))
        }
    };
    let fields = match str.fields {
        Fields::Named(n) => get_fields(&n.named)?,
        Fields::Unnamed(u) => get_fields(&u.unnamed)?,
        Fields::Unit => vec![],
    };
    let name = input.ident;
    let num_fields = fields.len();
    let num_unique_names = fields
        .iter()
        .filter_map(|f| match &f.column {
            ColumnIdentifier::Name(n) => Some(n.value()),
            _ => None,
        })
        .collect::<HashSet<_>>()
        .len();
    let body = if num_unique_names == 0 {
        let mut ret = vec![];
        for field in fields.iter() {
            if let ColumnIdentifier::Index(idx) = &field.column {
                ret.push(quote!(#idx));
            }
        }
        quote! {
            [#(#ret,)*]
        }
    } else if num_unique_names == 1 {
        let mut ret = vec![];
        for field in fields.iter() {
            match &field.column {
                ColumnIdentifier::Index(idx) => ret.push(quote!(#idx)),
                ColumnIdentifier::Name(_) => ret.push(quote!(column_idx)),
            }
        }
        let name = fields
            .iter()
            .filter_map(|f| match &f.column {
                ColumnIdentifier::Index(_) => None,
                ColumnIdentifier::Name(n) => Some(n),
            })
            .next()
            .unwrap();
        let error = format!("There is no column named `{}`", name.value());
        quote! {
            for (column_idx, column) in row.columns().iter().enumerate() {
                if column.name() == #name {
                    return [#(#ret,)*];
                }
            }
            ::std::panic::panic_any(#error);
        }
    } else {
        let mut names = HashMap::new();
        let mut init = vec![];
        let mut missing_body = vec![];
        let mut repeats = vec![];
        for (idx, field) in fields.iter().enumerate() {
            match &field.column {
                ColumnIdentifier::Index(idx) => {
                    init.push(quote!(#idx));
                }
                ColumnIdentifier::Name(n) => {
                    init.push(quote!(!0));
                    let value = n.value();
                    let entry = names
                        .entry(value.len())
                        .or_insert_with(HashMap::new)
                        .entry(value.clone());
                    match entry {
                        Entry::Vacant(e) => {
                            e.insert((n, idx));
                            let error = format!("There is no column named `{}`", n.value());
                            missing_body.push(quote! {
                                if entries[#idx] == !0 {
                                    #error
                                }
                            });
                        }
                        Entry::Occupied(e) => {
                            let original = e.get().1;
                            repeats.push(quote! {
                                columns[#idx] = columns[#original];
                            })
                        }
                    }
                }
            }
        }
        let mut names: Vec<_> = names.into_iter().collect();
        names.sort_by_key(|n| n.0);
        let mut outer_match_body = vec![];
        for (len, matches) in names {
            let inner_match = generate_length_group_body(len, &matches);
            outer_match_body.push(quote! {
                #len => #inner_match
            })
        }
        quote! {
            let mut columns = [#(#init,)*];
            let mut todo = #num_unique_names;
            for (column_idx, column) in row.columns().iter().enumerate() {
                let name = column.name();
                let idx = match name.len() {
                    #(#outer_match_body,)*
                    _ => continue,
                };
                if columns[idx] == !0 {
                    columns[idx] = column_idx;
                    todo -= 1;
                    if todo == 0 {
                        break;
                    }
                }
            }
            #[cold]
            fn missing(entries: &[usize; #num_fields]) -> ! {
                let msg = #(
                    #missing_body
                ) else * else {
                    "entered unreachable code"
                };
                ::std::panic::panic_any(msg);
            }
            if todo > 0 {
                missing(&columns);
            }
            #(#repeats)*
            columns
        }
    };
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::tokio_postgres_extractor::Columns for #name #type_generics #where_clause {
            type Columns = [usize; #num_fields];

            fn columns(row: &::tokio_postgres_extractor::private::tokio_postgres::Row) -> Self::Columns {
                #body
            }
        }
    })
}

fn generate_length_group_body(
    len: usize,
    names: &HashMap<String, (&LitStr, usize)>,
) -> TokenStream {
    let mut names: Vec<_> = names.iter().map(|v| (v.0, v.1 .0, v.1 .1)).collect();
    names.sort_by_key(|v| v.0);
    if names.len() == 1 || matches!(len, 1 | 2 | 4 | 8) {
        return generate_fallback_length_group_body(&names);
    }
    let mut unique = HashSet::new();
    for sub_len_shift in 0..4 {
        let sub_len = 1 << sub_len_shift;
        'outer: for start in 0..=len - sub_len {
            unique.clear();
            for (name, _, _) in &names {
                if !unique.insert(&name.as_bytes()[start..start + sub_len]) {
                    continue 'outer;
                }
            }
            let s = start;
            let s1 = s + 1;
            let s2 = s + 2;
            let s3 = s + 3;
            let s4 = s + 4;
            let s5 = s + 5;
            let s6 = s + 6;
            let s7 = s + 7;
            let e0 = |b: &[u8]| b[s] as u64;
            let e1 = |b: &[u8]| u16::from_le_bytes([b[s], b[s1]]) as u64;
            let e2 = |b: &[u8]| u32::from_le_bytes([b[s], b[s1], b[s2], b[s3]]) as u64;
            let e3 = |b: &[u8]| {
                u64::from_le_bytes([b[s], b[s1], b[s2], b[s3], b[s4], b[s5], b[s6], b[s7]])
            };
            #[allow(clippy::type_complexity)]
            let (extract_rt, extract_ct): (_, &dyn Fn(&[u8]) -> u64) = match sub_len_shift {
                0 => {
                    let rt = quote!(b[#start]);
                    (rt, &e0)
                }
                1 => {
                    let rt = quote!(u16::from_le_bytes([b[#s], b[#s1]]));
                    (rt, &e1)
                }
                2 => {
                    let rt = quote!(u32::from_le_bytes([b[#s], b[#s1], b[#s2], b[#s3]]));
                    (rt, &e2)
                }
                3 => {
                    let rt = quote!(u64::from_le_bytes([b[#s], b[#s1], b[#s2], b[#s3], b[#s4], b[#s5], b[#s6], b[#s7]]));
                    (rt, &e3)
                }
                _ => unreachable!(),
            };
            let mut disc_match_body = vec![];
            for (name, lit, idx) in &names {
                let disc = extract_ct(name.as_bytes());
                let disc = Literal::u64_unsuffixed(disc);
                disc_match_body.push(quote! {
                    #disc => match name {
                        #lit => #idx,
                        _ => continue,
                    }
                })
            }
            return quote! {{
                let b = name.as_bytes();
                let disc = #extract_rt;
                match disc {
                    #(#disc_match_body,)*
                    _ => continue,
                }
            }};
        }
    }
    generate_fallback_length_group_body(&names)
}

fn generate_fallback_length_group_body(names: &[(&String, &LitStr, usize)]) -> TokenStream {
    let mut inner_match_body = vec![];
    for (_, name, idx) in names {
        inner_match_body.push(quote! {
            #name => #idx
        })
    }
    quote! {
        match name {
            #(#inner_match_body,)*
            _ => continue,
        }
    }
}

enum ColumnIdentifier {
    Index(Expr),
    Name(LitStr),
}

struct ColumnField {
    column: ColumnIdentifier,
}

fn get_fields(fields: &Punctuated<Field, Token![,]>) -> Result<Vec<ColumnField>, Error> {
    let mut res = vec![];
    for (field_idx, field) in fields.iter().enumerate() {
        let attr = get_column_attr(&field.attrs)?;
        let column = match (attr.idx, attr.name) {
            (Some(_), Some(_)) => {
                return Err(Error::new(
                    attr.span.unwrap(),
                    "Cannot specify both `idx` and `name`",
                ))
            }
            (Some(idx), _) => ColumnIdentifier::Index(idx),
            (_, Some(rename)) => ColumnIdentifier::Name(rename),
            _ => match field.ident.clone() {
                None => ColumnIdentifier::Index(parse_quote_spanned! {
                    field.span() => #field_idx
                }),
                Some(ident) => {
                    let ident_str = ident.to_string();
                    let ident_str = ident_str.strip_prefix("r#").unwrap_or(&ident_str);
                    ColumnIdentifier::Name(parse_quote_spanned! {
                        ident.span() => #ident_str
                    })
                }
            },
        };
        res.push(ColumnField { column });
    }
    Ok(res)
}

const COLUMN_ATTR: &str = "column";

#[derive(Default)]
struct ColumnAttr {
    span: Option<Span>,
    idx: Option<Expr>,
    name: Option<LitStr>,
}

fn get_column_attr(attrs: &[Attribute]) -> Result<ColumnAttr, Error> {
    let mut cattr = ColumnAttr::default();
    for attr in attrs {
        match &attr.meta {
            Meta::Path(p) => assert_not_column_attr(p)?,
            Meta::NameValue(n) => assert_not_column_attr(&n.path)?,
            Meta::List(l) if l.path.is_ident(COLUMN_ATTR) => {
                cattr.span = Some(match cattr.span {
                    None => l.tokens.span(),
                    Some(s) => l.tokens.span().join(s).unwrap_or(s),
                });
                let values =
                    Punctuated::<Meta, Token![,]>::parse_terminated.parse2(l.tokens.clone())?;
                for meta in values {
                    match meta {
                        Meta::NameValue(n) => {
                            if n.path.is_ident("idx") {
                                if cattr.idx.is_some() {
                                    return Err(Error::new_spanned(
                                        n.path,
                                        "`idx` attribute specified multiple times",
                                    ));
                                }
                                cattr.idx = Some(n.value);
                            } else if n.path.is_ident("name") {
                                if cattr.name.is_some() {
                                    return Err(Error::new_spanned(
                                        n.path,
                                        "`name` attribute specified multiple times",
                                    ));
                                }
                                let name = 'name: {
                                    if let Expr::Lit(lit) = &n.value {
                                        if let Lit::Str(s) = &lit.lit {
                                            break 'name s.clone();
                                        }
                                    }
                                    return Err(Error::new_spanned(
                                        n.value,
                                        "`name` attribute value must be a string literal",
                                    ));
                                };
                                cattr.name = Some(name);
                            } else {
                                return Err(Error::new_spanned(n.path, "Unknown attribute"));
                            }
                        }
                        _ => return Err(Error::new_spanned(meta, "Unknown attribute")),
                    }
                }
            }
            Meta::List(_) => {}
        }
    }
    Ok(cattr)
}

fn assert_not_column_attr(path: &Path) -> Result<(), Error> {
    if path.is_ident(COLUMN_ATTR) {
        let msg = format!("`{COLUMN_ATTR}` attribute must be a list attribute: `{COLUMN_ATTR}()`");
        return Err(Error::new_spanned(path, msg));
    }
    Ok(())
}
