// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use proc_macro::TokenStream;
use quote::quote;
use syn;

/// The macro can only be used in integration test module
#[proc_macro_attribute]
pub fn ob_versions_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemFn);
    let name = &input.sig.ident;
    let body = &input.block;
    let result = quote! {
        fn #name() {
            use common;
            for v in common::OB_VERSIONS.iter() {
                let client = common::build_normal_client(*v);
                #body
            }
        }
    };
    proc_macro::TokenStream::from(result)
}

/// The macro can only be used in integration test module
#[proc_macro_attribute]
pub fn hbase_ob_versions_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemFn);
    let name = &input.sig.ident;
    let body = &input.block;
    let result = quote! {
        fn #name() {
            use common;
            for v in common::OB_VERSIONS.iter() {
                let client = common::build_hbase_client(*v);
                #body
            }
        }
    };
    proc_macro::TokenStream::from(result)
}
