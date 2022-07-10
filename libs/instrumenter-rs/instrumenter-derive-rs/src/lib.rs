use proc_macro::TokenStream;
use quote::quote;
use syn::__private::TokenStream2;
use syn::{parse_macro_input, parse_quote, ItemFn};

#[proc_macro_attribute]
pub fn track(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut function = parse_macro_input!(input as ItemFn);
    let args = TokenStream2::from(args);

    function
        .attrs
        .push(parse_quote!(#[instrumenter::private__::instrument(skip_all, #args)]));

    TokenStream::from(quote! {
        #function
    })
}
