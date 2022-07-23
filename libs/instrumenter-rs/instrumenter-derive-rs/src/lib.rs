#![allow(warnings)]

use proc_macro::TokenStream;
#[cfg(feature = "enabled")]
use proc_macro2::TokenStream as TokenStream2;
#[cfg(feature = "enabled")]
use quote::quote;
#[cfg(feature = "enabled")]
use syn::{parse_macro_input, parse_quote, ItemFn};

#[proc_macro_attribute]
pub fn track(args: TokenStream, input: TokenStream) -> TokenStream {
    #[cfg(feature = "enabled")]
    {
        let mut function = parse_macro_input!(input as ItemFn);
        let args = TokenStream2::from(args);

        function
            .attrs
            .push(parse_quote!(#[instrumenter::private__::instrument(skip_all, #args)]));
        function.block.stmts.insert(
            0,
            parse_quote!(
                use instrumenter::private__ as tracing;
            ),
        );

        TokenStream::from(quote! {
            #function
        })
    }

    #[cfg(not(feature = "enabled"))]
    input
}
