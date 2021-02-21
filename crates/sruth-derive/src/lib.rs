use derive_utils::quick_derive;
use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::{parse_macro_input, spanned::Spanned, Data, DeriveInput, Error, Ident, Index, Result};

#[proc_macro_derive(NodeExt)]
pub fn derive_node_ext(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    quick_derive! {
        input,
        NodeExt,
        trait NodeExt {
            fn node_name(&self) -> &'static str;
            fn evaluate_with_constants(self, constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>);
        }
    }
}

#[proc_macro_derive(Castable)]
pub fn derive_castable_stub(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    derive_castable(input)
        .unwrap_or_else(|err| err.into_compile_error())
        .into()
}

fn derive_castable(input: DeriveInput) -> Result<TokenStream> {
    let target_enum = if let Data::Enum(target) = input.data {
        target
    } else {
        return Err(Error::new_spanned(input, "Castable only accepts enums"));
    };
    let enum_ty = input.ident;
    let mut output = TokenStream::new();

    for variant in target_enum.variants.iter() {
        let mut fields = variant.fields.iter();
        let field = fields.next().unwrap();
        assert_eq!(fields.count(), 0);

        let variant_name = &variant.ident;
        let field_ty = &field.ty;
        let field_name = field.ident.as_ref().map_or_else(
            || {
                Index {
                    index: 0,
                    span: field.span(),
                }
                .into_token_stream()
            },
            |field| field.into_token_stream(),
        );
        let value = Ident::new("value", Span::mixed_site());

        output.extend(quote! {
            impl Castable<#field_ty> for #enum_ty {
                fn is(&self) -> bool {
                    core::matches!(self, #enum_ty::#variant_name { .. })
                }

                unsafe fn cast_unchecked(&self) -> &#field_ty {
                    if let #enum_ty::#variant_name { #field_name: #value } = self {
                        #value
                    } else {
                        core::hint::unreachable_unchecked()
                    }
                }
            }
        });
    }

    Ok(output)
}
