use {
    proc_macro2::{Ident, Span, TokenStream},
    quote::quote,
    std::collections::HashSet,
    syn::{
        parse_quote_spanned, punctuated::Punctuated, spanned::Spanned, Data, DeriveInput, Error,
        Field, Fields, GenericParam, Lifetime, LifetimeParam, Token, Type, WhereClause,
    },
};

pub fn extract_impl(input: DeriveInput) -> Result<TokenStream, Error> {
    let str = match input.data {
        Data::Struct(s) => s,
        _ => {
            return Err(Error::new_spanned(
                input,
                "`Extract` can only be derive for structs",
            ))
        }
    };
    let (body, types) = match str.fields {
        Fields::Named(named) => {
            let (body, types) = fields(&named.named);
            (quote!(Self { #(#body,)* }), types)
        }
        Fields::Unnamed(unnamed) => {
            let (body, types) = fields(&unnamed.unnamed);
            (quote!(Self(#(#body),*)), types)
        }
        Fields::Unit => (quote!(Self), vec![]),
    };
    let (_, type_generics, _) = input.generics.split_for_impl();
    let mut modified_generics = input.generics.clone();
    let lifetimes: Vec<_> = modified_generics
        .params
        .iter()
        .filter_map(|l| match l {
            GenericParam::Lifetime(l) => Some(l),
            _ => None,
        })
        .map(|l| &l.lifetime)
        .collect();
    let mut row_lt_name = String::new();
    'outer: for idx in 0.. {
        row_lt_name = format!("row{idx}");
        for lt in &lifetimes {
            if lt.ident == row_lt_name {
                continue 'outer;
            }
        }
        break;
    }
    let row_lt = Lifetime {
        apostrophe: Span::call_site(),
        ident: Ident::new(&row_lt_name, Span::call_site()),
    };
    if types.len() > 0 {
        let where_clause = modified_generics.where_clause.get_or_insert(WhereClause {
            where_token: Default::default(),
            predicates: Default::default(),
        });
        for ty in &types {
            where_clause.predicates.push(parse_quote_spanned!(
                ty.span() => #ty: ::tokio_postgres_extractor::private::tokio_postgres::types::FromSql<#row_lt>
            ))
        }
    }
    modified_generics
        .params
        .push(GenericParam::Lifetime(LifetimeParam::new(row_lt.clone())));
    let (impl_generics, _, where_clause) = modified_generics.split_for_impl();
    let name = input.ident;
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::tokio_postgres_extractor::Extract<#row_lt> for #name #type_generics #where_clause {
            fn extract_with_columns(
                columns: &Self::Columns,
                row: &#row_lt ::tokio_postgres_extractor::private::tokio_postgres::Row,
            ) -> Self {
                #body
            }
        }
    })
}

fn fields(input: &Punctuated<Field, Token![,]>) -> (Vec<TokenStream>, Vec<Type>) {
    let mut fields = vec![];
    let mut unique_types = HashSet::new();
    let mut types = vec![];
    for (idx, field) in input.iter().enumerate() {
        let get = quote!(row.get(columns[#idx]));
        fields.push(match &field.ident {
            None => get,
            Some(ident) => quote!(#ident: #get),
        });
        if unique_types.insert(field.ty.clone()) {
            types.push(field.ty.clone());
        }
    }
    (fields, types)
}
