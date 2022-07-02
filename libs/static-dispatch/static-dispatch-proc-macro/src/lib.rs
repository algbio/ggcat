extern crate proc_macro;
extern crate quote;

use proc_macro2::{Ident, Span, TokenStream};
use proc_macro_error::{abort, proc_macro_error};
use quote::{quote, ToTokens};
use std::collections::HashMap;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, parse_quote, Expr, ExprArray, GenericParam, ItemFn, ItemImpl, ItemTrait,
    Token, Type, TypeParamBound,
};
use syn::{FnArg, Item};

struct FunctionSpecializations {
    specs: Vec<(String, ExprArray)>,
}

impl Parse for FunctionSpecializations {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut specs = Vec::new();

        while !input.is_empty() {
            let name: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let array = input.parse()?;
            specs.push((name.to_string(), array));
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }
        Ok(FunctionSpecializations { specs })
    }
}

fn static_dispatch_fn(args: FunctionSpecializations, function: ItemFn) -> TokenStream {
    let mut generics_list = Vec::new();

    let mut attr_params = HashMap::new();
    for (name, arg) in args.specs {
        attr_params.insert(name, arg);
    }

    for param in function.sig.generics.params.clone() {
        let (name, const_type, first_bound) = match param.clone() {
            GenericParam::Type(ty) => {
                let first_bound = ty
                    .bounds
                    .iter()
                    .filter(|x| {
                        if let TypeParamBound::Trait(_) = x {
                            true
                        } else {
                            false
                        }
                    })
                    .next()
                    .expect("At least one bound for each generic parametere must be specified.")
                    .to_token_stream();

                (ty.ident.to_string(), None, Some(first_bound))
            }
            GenericParam::Const(cs) => (cs.ident.to_string(), Some(cs.ty), None),
            GenericParam::Lifetime(_) => continue, // Ignored
        };

        let names: Vec<_> = match attr_params.get(&name) {
            None => {
                abort!(
                    param.span(),
                    "Static dispatch not specified for generic attribute '{}'",
                    name
                );
            }
            Some(names) => names.elems.clone().into_iter().collect(),
        };

        generics_list.push((name, names, const_type, param.clone(), first_bound));
    }

    let fn_name = function.sig.ident.clone();
    let static_fn_name = Ident::new(
        &format!("{}_static", function.sig.ident),
        function.sig.ident.span(),
    );

    let dynamic_dispatch_fn_name = Ident::new(
        &format!("__{}_static", function.sig.ident),
        function.sig.ident.span(),
    );

    let fn_args = function.sig.inputs.clone();
    let fn_args_pass: Vec<_> = function
        .sig
        .inputs
        .iter()
        .map(|x| match x {
            FnArg::Receiver(x) => x.self_token.to_token_stream(),
            FnArg::Typed(x) => x.pat.to_token_stream(),
        })
        .collect();
    let fn_rettype = function.sig.output.clone();

    let make_function_name = |name| {
        Ident::new(
            &format!("dispatch_fn_{}_{}", function.sig.ident.to_string(), name),
            function.sig.span(),
        )
    };

    let mut dispatch_traits = TokenStream::new();
    for (name, list, const_type, _, _) in &generics_list {
        if let Some(const_type) = const_type {
            let dispatch_function_name = make_function_name(name.clone());

            let mut match_branches = TokenStream::new();
            for (idx, value) in list.iter().enumerate() {
                (quote! {
                    #value => #idx,
                })
                .to_tokens(&mut match_branches);
            }

            (quote! {
                    #[allow(non_snake_case)]
                    #[doc(hidden)]
                    fn #dispatch_function_name(x: #const_type) -> usize {
                        match x {
                            #match_branches
                            _ => panic!(concat!("Const range for variable ", concat!(#name, " not supported!")))
                        }
                    }
                })
                .to_tokens(&mut dispatch_traits);
        }
    }

    let mut dispatch_generic_args = TokenStream::new();
    let mut dispatch_generic_args_pass = TokenStream::new();
    let mut dispatch_tuple_members = TokenStream::new();
    let mut dispatch_tuple_builders = TokenStream::new();

    for (name, _list, const_type, generic, first_bound) in &generics_list {
        let ident_name = Ident::new(&name, Span::call_site());

        if let Some(const_type) = const_type {
            let dispatch_function = make_function_name(name.clone());

            (quote! {
                const #ident_name: #const_type,
            })
            .to_tokens(&mut dispatch_generic_args);

            (quote! {
                #dispatch_function(#ident_name),
            })
            .to_tokens(&mut dispatch_tuple_builders);

            (quote! {
                usize,
            })
            .to_tokens(&mut dispatch_tuple_members);
        } else {
            (quote! {
                #generic,
            })
            .to_tokens(&mut dispatch_generic_args);

            (quote! {
                <#ident_name as #first_bound>::STATIC_DISPATCH_ID,
            })
            .to_tokens(&mut dispatch_tuple_builders);

            (quote! {
                ::static_dispatch::StaticDispatch<()>,
            })
            .to_tokens(&mut dispatch_tuple_members);
        }

        (quote! {
            #ident_name,
        })
        .to_tokens(&mut dispatch_generic_args_pass);
    }

    fn recursive_dispatch_builder(
        index: usize,
        gen_args: TokenStream,
        generics_list: &Vec<(
            String,
            Vec<Expr>,
            Option<Type>,
            GenericParam,
            Option<TokenStream>,
        )>,
        fn_name: &Ident,
        fn_args: &TokenStream,
    ) -> TokenStream {
        if index == generics_list.len() {
            quote! { #fn_name::<#gen_args>(#fn_args) }
        } else {
            let mut output_dispatcher = TokenStream::new();

            let is_const = generics_list[index].2.is_some();

            for (idx, ty) in generics_list[index].1.iter().enumerate() {
                let gen_args = if index == 0 {
                    quote! { #ty }
                } else {
                    quote! { #gen_args, #ty }
                };

                let nested = recursive_dispatch_builder(
                    index + 1,
                    gen_args,
                    generics_list,
                    fn_name,
                    fn_args,
                );

                if is_const {
                    quote! {
                        #idx => #nested,
                    }
                } else {
                    let first_bound = generics_list[index].4.as_ref().unwrap();

                    quote! {
                        <#ty as #first_bound>::STATIC_DISPATCH_ID => #nested,
                    }
                }
                .to_tokens(&mut output_dispatcher);
            }

            let tuple_index = syn::Index::from(index);

            quote! {
                match dispatch_tuple.#tuple_index {
                    #output_dispatcher
                    x => panic!("Static dispatch bug, arg {:?}!", x)
                }
            }
        }
    }

    let final_dispatcher = recursive_dispatch_builder(
        0,
        TokenStream::new(),
        &generics_list,
        &fn_name.clone(),
        &quote! { #(#fn_args_pass),* },
    );

    quote! {

        #dispatch_traits

        #[doc(hidden)]
        #[inline(always)]
        fn __dispatch<#dispatch_generic_args>() -> (#dispatch_tuple_members) {
            (#dispatch_tuple_builders)
        }

        #[doc(hidden)]
        #[inline(never)]
        pub fn #dynamic_dispatch_fn_name(dispatch_tuple: (#dispatch_tuple_members), #fn_args) #fn_rettype {
             #final_dispatcher
        }

        #[doc(hidden)]
        #[inline(always)]
        pub fn #static_fn_name<#dispatch_generic_args>(#fn_args) #fn_rettype {
            let dispatch_tuple = __dispatch::<#dispatch_generic_args_pass>();
            #dynamic_dispatch_fn_name(dispatch_tuple, #(#fn_args_pass),*)
        }

        pub mod static_dispatch {
            pub use super::#static_fn_name as #fn_name;
        }

        pub mod dynamic_dispatch {
            pub use super::#dynamic_dispatch_fn_name as #fn_name;
        }

    }
}

fn static_dispatch_trait(mut trait_: ItemTrait) -> TokenStream {
    trait_
        .items
        .push(parse_quote! { const STATIC_DISPATCH_ID: ::static_dispatch::StaticDispatch<()>; });

    trait_.to_token_stream()
}

fn static_dispatch_impl(mut impl_: ItemImpl) -> TokenStream {
    impl_.impl_token;

    impl_.items.push(parse_quote! {
        const STATIC_DISPATCH_ID: ::static_dispatch::StaticDispatch::<()> =
            ::static_dispatch::StaticDispatch::<()> { value: std::any::TypeId::of::<Self>(), _phantom: std::marker::PhantomData };
    });

    impl_.to_token_stream()
}

#[proc_macro_error]
#[proc_macro_attribute]
pub fn static_dispatch(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input_ = input.clone();
    let function = parse_macro_input!(input_ as Item);
    let input = proc_macro2::TokenStream::from(input);

    let (input, static_dispatch_module) = match function {
        Item::Fn(function) => {
            let args = parse_macro_input!(args as FunctionSpecializations);
            (input, static_dispatch_fn(args, function))
        }
        Item::Trait(trait_) => (TokenStream::new(), static_dispatch_trait(trait_)),
        Item::Impl(impl_) => (TokenStream::new(), static_dispatch_impl(impl_)),
        _ => {
            panic!(
                "static_dispatch attribute is applicable only to functions, traits or trait impls."
            );
        }
    };

    // panic!("{}", static_dispatch_module.to_string());

    quote!(#input #static_dispatch_module).into()
}
