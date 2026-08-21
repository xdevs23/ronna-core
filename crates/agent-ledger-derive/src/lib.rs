//! The composing-enum derive for `agent-ledger`.
//!
//! A consumer of the ledger runtime composes its own block kinds with the
//! library's by writing an enum — one variant per kind of its own, plus one
//! variant holding the inner implementor the parse chain falls back to — and
//! deriving `Agency` on it. The derive generates exactly the delegation the
//! library maintains by hand for its own `BlockKind`: the `Agency` impl, the
//! `Projection` impl, the `FromBlock` parse chain, and the concatenated
//! descriptor set the store is configured with. One derive, both traits, and
//! no hand-written dispatch anywhere in a consumer.
//!
//! This crate is re-exported by `agent-ledger` itself, so a consumer writes
//! `use agent_ledger::Agency;` once and has the trait and the derive under one
//! name — depending on this crate directly buys nothing.

use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Error, Fields, Ident, Path, Type, parse_macro_input};

/// One composed leaf variant: its name and the kind it holds.
struct Leaf<'a> {
    ident: &'a Ident,
    ty: &'a Type,
}

/// Derive the whole composing-enum surface: `Agency`, `Projection`,
/// `FromBlock` and the descriptor concatenation, from one enum and one
/// attribute.
///
/// # Shape
///
/// Every variant holds exactly one field — the kind it composes. Exactly one
/// variant carries `#[agency(delegate)]`: it holds the inner `FromBlock`
/// implementor (the library's `BlockKind`, or another composed enum —
/// composition nests), and it is where the parse chain sends any type string
/// no leaf claims, so the inert fallback stays exactly where the inner
/// implementor put it. Every other variant holds a leaf kind implementing
/// `LeafKind`, whose `KINDS` const the generated parse tries in declaration
/// order.
///
/// ```
/// use agent_ledger::{Agency, Block, BlockKind, FromBlock, LeafKind, Projection};
///
/// #[derive(Debug, Clone)]
/// struct Note {
///     body: String,
/// }
///
/// impl LeafKind for Note {
///     const KINDS: &'static [&'static str] = &["note"];
///
///     fn parse(block: &Block) -> Self {
///         Self {
///             body: block
///                 .fields
///                 .get("body")
///                 .and_then(|value| value.as_str())
///                 .unwrap_or_default()
///                 .to_string(),
///         }
///     }
/// }
///
/// impl Agency for Note {}
/// impl Projection for Note {}
///
/// #[derive(Agency)]
/// enum MyKind {
///     #[agency(delegate)]
///     Core(BlockKind),
///     Note(Note),
/// }
///
/// let mut fields = serde_json::Map::new();
/// fields.insert("body".into(), serde_json::Value::String("hello".into()));
/// let block = Block {
///     id: 1,
///     role: None,
///     block_type: "note".into(),
///     created_at: String::new(),
///     fields,
/// };
/// // The consumer's kind resolves through its own parse…
/// assert!(matches!(MyKind::from_block(&block), MyKind::Note(ref n) if n.body == "hello"));
/// // …and the library's kinds resolve through the delegate, untouched.
/// let core = Block {
///     block_type: "text".into(),
///     ..block
/// };
/// assert!(matches!(MyKind::from_block(&core), MyKind::Core(BlockKind::Text(_))));
/// // The composed claim is the union: the delegate's stored strings plus the
/// // leaf's, which is what lets a further composition around `MyKind` check
/// // its own leaves against everything already claimed here.
/// assert!(MyKind::CLAIMED_KINDS.contains(&"text"));
/// assert!(MyKind::CLAIMED_KINDS.contains(&"note"));
/// ```
///
/// # One owner per stored string
///
/// The generated parse chain is first-match, so a stored string with two
/// owners would resolve silently to whichever variant is declared first — a
/// shadowing no test would see. The derive refuses to build one: every leaf's
/// `KINDS` is checked disjoint from the delegate's `CLAIMED_KINDS` and from
/// every sibling leaf's, in constant evaluation, and the composed enum's own
/// `CLAIMED_KINDS` is the union of all of them — so nesting a composed enum
/// inside another checks the whole claim transitively. In the same pass,
/// every stored string a leaf's descriptors claim must be in that leaf's
/// `KINDS`: the write path keys off the descriptor's list and the read path
/// keys off the leaf's, and a string in one but not the other is a row the
/// store would write and the parse chain would hand to the inert fallback.
///
/// # Durability
///
/// A kind's `durable()` answer is the single source of its rows' lifetime,
/// and the derive delegates it to the leaf exactly like every other hook —
/// there is no attribute to restate it with. Its agreement with the
/// descriptors' `ephemeral` flag is the conformance check's job at test time:
/// run `check_descriptor_durability` over the composed enum and its
/// descriptor set.
///
/// # Descriptors
///
/// The generated `FromBlock::DESCRIPTORS` is the compile-time concatenation of
/// the delegate's `DESCRIPTORS` and every leaf's, in declaration order. Hand
/// it to the store as the configured descriptor set; the set the store
/// validates and the set the parse chain resolves are then one declaration.
///
/// # The crate path
///
/// The generated code reaches the library as `::agent_ledger`. A consumer
/// that depends on it under another name — a renamed dependency, a re-export
/// through a facade — names the path it is reachable under instead, on the
/// enum:
///
/// ```
/// use agent_ledger as the_ledger;
/// use the_ledger::{Agency, BlockKind};
///
/// #[derive(Agency)]
/// #[agency(crate = the_ledger)]
/// enum MyKind {
///     #[agency(delegate)]
///     Core(BlockKind),
/// }
/// ```
///
/// The path must lead to the library's root (or a re-export of it): the
/// generated code resolves the traits and the runtime types through it.
///
/// # Rejections
///
/// A variant whose type does not implement `LeafKind` (with `Agency` and
/// `Projection`) fails with E0277:
///
/// ```compile_fail,E0277
/// use agent_ledger::{Agency, BlockKind};
///
/// struct Bare;
///
/// #[derive(Agency)]
/// enum MyKind {
///     #[agency(delegate)]
///     Core(BlockKind),
///     Bare(Bare),
/// }
/// ```
///
/// A leaf claiming a stored string the delegate already claims fails constant
/// evaluation with E0080 — the collision would otherwise shadow the library's
/// own kind in the first-match parse chain, silently:
///
/// ```compile_fail,E0080
/// use agent_ledger::{Agency, Block, BlockKind, LeafKind, Projection};
///
/// struct Shadow;
///
/// impl LeafKind for Shadow {
///     // "text" is the library's: the delegate's CLAIMED_KINDS already
///     // holds it, so this composition cannot build.
///     const KINDS: &'static [&'static str] = &["text"];
///     fn parse(_: &Block) -> Self {
///         Self
///     }
/// }
/// impl Agency for Shadow {}
/// impl Projection for Shadow {}
///
/// #[derive(Agency)]
/// enum MyKind {
///     #[agency(delegate)]
///     Core(BlockKind),
///     Shadow(Shadow),
/// }
/// ```
///
/// Two leaves claiming one stored string fail the same way:
///
/// ```compile_fail,E0080
/// use agent_ledger::{Agency, Block, BlockKind, LeafKind, Projection};
///
/// struct First;
/// struct Second;
///
/// impl LeafKind for First {
///     const KINDS: &'static [&'static str] = &["note"];
///     fn parse(_: &Block) -> Self {
///         Self
///     }
/// }
/// impl Agency for First {}
/// impl Projection for First {}
///
/// impl LeafKind for Second {
///     const KINDS: &'static [&'static str] = &["note"];
///     fn parse(_: &Block) -> Self {
///         Self
///     }
/// }
/// impl Agency for Second {}
/// impl Projection for Second {}
///
/// #[derive(Agency)]
/// enum MyKind {
///     #[agency(delegate)]
///     Core(BlockKind),
///     First(First),
///     Second(Second),
/// }
/// ```
///
/// A leaf whose descriptors claim a stored string missing from its `KINDS`
/// fails constant evaluation with E0080 — the store would accept the write
/// and the parse chain would hand the row to the inert fallback:
///
/// ```compile_fail,E0080
/// use agent_ledger::{
///     Agency, Block, BlockKind, Column, ColumnType, ContentDescriptor, LeafKind, Projection,
/// };
///
/// struct Note;
///
/// impl LeafKind for Note {
///     const KINDS: &'static [&'static str] = &["note"];
///     const DESCRIPTORS: &'static [ContentDescriptor] = &[ContentDescriptor {
///         table: "block_note",
///         domain: "notes",
///         // "note_extra" has no parse: refused.
///         kinds: &["note", "note_extra"],
///         columns: &[Column::new("body", ColumnType::Text)],
///         reference_columns: &[],
///         ephemeral: false,
///     }];
///     fn parse(_: &Block) -> Self {
///         Self
///     }
/// }
/// impl Agency for Note {}
/// impl Projection for Note {}
///
/// #[derive(Agency)]
/// enum MyKind {
///     #[agency(delegate)]
///     Core(BlockKind),
///     Note(Note),
/// }
/// ```
///
/// An `#[agency(...)]` key outside the accepted set — `delegate` on the
/// fallback variant, `crate = path` on the enum — is refused where it is
/// written, naming that set:
///
/// ```compile_fail
/// use agent_ledger::{Agency, Block, BlockKind, LeafKind, Projection};
///
/// struct Tail;
///
/// impl LeafKind for Tail {
///     const KINDS: &'static [&'static str] = &["tail"];
///     fn parse(_: &Block) -> Self {
///         Self
///     }
/// }
/// impl Agency for Tail {}
/// impl Projection for Tail {}
///
/// #[derive(Agency)]
/// enum MyKind {
///     #[agency(delegate)]
///     Core(BlockKind),
///     // A row's lifetime is the kind's own durable() answer; there is no
///     // attribute to restate it with.
///     #[agency(ephemeral)]
///     Tail(Tail),
/// }
/// ```
///
/// No `#[agency(delegate)]` variant — the parse chain has no fallback:
///
/// ```compile_fail
/// use agent_ledger::{Agency, BlockKind};
///
/// #[derive(Agency)]
/// enum MyKind {
///     Core(BlockKind),
/// }
/// ```
///
/// Two delegate variants — the parse chain has exactly one fallback:
///
/// ```compile_fail
/// use agent_ledger::{Agency, BlockKind};
///
/// #[derive(Agency)]
/// enum MyKind {
///     #[agency(delegate)]
///     A(BlockKind),
///     #[agency(delegate)]
///     B(BlockKind),
/// }
/// ```
///
/// Anything but an enum of single-field variants:
///
/// ```compile_fail
/// use agent_ledger::Agency;
///
/// #[derive(Agency)]
/// struct NotAnEnum;
/// ```
///
/// A duplicate crate path — the path is declared once, never resolved by
/// precedence:
///
/// ```compile_fail
/// use agent_ledger::{Agency, BlockKind};
///
/// #[derive(Agency)]
/// #[agency(crate = ::agent_ledger, crate = ::agent_ledger)]
/// enum MyKind {
///     #[agency(delegate)]
///     Core(BlockKind),
/// }
/// ```
#[proc_macro_derive(Agency, attributes(agency))]
pub fn derive_agency(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// The composed enum, parsed and checked: the library's path, one delegate,
/// any number of leaves.
struct Composition<'a> {
    krate: Path,
    delegate_ident: &'a Ident,
    delegate_ty: &'a Type,
    leaves: Vec<Leaf<'a>>,
}

impl Composition<'_> {
    /// Every variant name, delegate first — the dispatch order of every
    /// generated match.
    fn all_idents(&self) -> Vec<&Ident> {
        std::iter::once(self.delegate_ident)
            .chain(self.leaves.iter().map(|leaf| leaf.ident))
            .collect()
    }
}

/// The whole expansion, or the first defect found, precisely spanned.
fn expand(input: &DeriveInput) -> Result<proc_macro2::TokenStream, Error> {
    let composition = compose(input)?;
    let name = &input.ident;
    let coherence = coherence_asserts(&composition);
    let from_block = from_block_impl(name, &composition);
    let agency = agency_impl(name, &composition);
    let projection = projection_impl(name, &composition);
    Ok(quote! {
        #coherence
        #from_block
        #agency
        #projection
    })
}

/// Read the enum's shape into a [`Composition`], refusing everything the
/// derive's contract rules out.
fn compose(input: &DeriveInput) -> Result<Composition<'_>, Error> {
    let Data::Enum(data) = &input.data else {
        return Err(Error::new(
            input.ident.span(),
            "#[derive(Agency)] composes an enum: one variant per kind, plus one \
             #[agency(delegate)] variant holding the inner implementor",
        ));
    };
    if !input.generics.params.is_empty() {
        return Err(Error::new(
            input.generics.span(),
            "#[derive(Agency)] takes a concrete enum; a composed kind set has no \
             generic parameters",
        ));
    }
    let krate = crate_path(input)?;

    let mut delegate: Option<(&Ident, &Type)> = None;
    let mut leaves: Vec<Leaf<'_>> = Vec::new();

    for variant in &data.variants {
        let ty = variant_payload(variant)?;
        if variant_is_delegate(variant)? {
            if delegate.is_some() {
                return Err(Error::new(
                    variant.span(),
                    "a second #[agency(delegate)] variant; the parse chain has exactly \
                     one fallback",
                ));
            }
            delegate = Some((&variant.ident, ty));
        } else {
            leaves.push(Leaf {
                ident: &variant.ident,
                ty,
            });
        }
    }

    let Some((delegate_ident, delegate_ty)) = delegate else {
        return Err(Error::new(
            input.ident.span(),
            "no #[agency(delegate)] variant; mark the variant holding the inner \
             implementor (the library's BlockKind, or another composed enum), which is \
             where an unrecognised type string falls back to",
        ));
    };

    Ok(Composition {
        krate,
        delegate_ident,
        delegate_ty,
        leaves,
    })
}

/// The path the generated code reaches the library under: the enum-level
/// `#[agency(crate = path)]` override, or `::agent_ledger`. The override is
/// what keeps a renamed dependency from locking a consumer out of the derive.
fn crate_path(input: &DeriveInput) -> Result<Path, Error> {
    let mut path: Option<Path> = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("agency") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                // A second crate path must refuse, not silently win: two
                // declarations with an undocumented precedence is the shape
                // the ephemerality attribute was removed for, and the
                // duplicate-delegate check already refuses its twin loudly.
                if path.is_some() {
                    return Err(
                        meta.error("duplicate #[agency(crate = ...)]; declare the path once")
                    );
                }
                path = Some(meta.value()?.parse()?);
                Ok(())
            } else {
                Err(meta.error(
                    "unknown #[agency(...)] key on the enum; the accepted set is \
                     `crate = path` here and `delegate` on the fallback variant",
                ))
            }
        })?;
    }
    Ok(path.unwrap_or_else(|| syn::parse_quote!(::agent_ledger)))
}

/// The one field a variant composes, or the refusal naming the shape rule.
fn variant_payload(variant: &syn::Variant) -> Result<&Type, Error> {
    if let Fields::Unnamed(fields) = &variant.fields
        && fields.unnamed.len() == 1
        && let Some(field) = fields.unnamed.first()
    {
        return Ok(&field.ty);
    }
    Err(Error::new(
        variant.span(),
        "every variant holds exactly one unnamed field: the kind it composes",
    ))
}

/// Whether the variant carries `#[agency(delegate)]`, with any key outside
/// the accepted set refused where it is written, naming that set.
fn variant_is_delegate(variant: &syn::Variant) -> Result<bool, Error> {
    let mut is_delegate = false;
    for attr in &variant.attrs {
        if !attr.path().is_ident("agency") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("delegate") {
                is_delegate = true;
                Ok(())
            } else {
                Err(meta.error(
                    "unknown #[agency(...)] key; the accepted set is `delegate` on the \
                     fallback variant and `crate = path` on the enum",
                ))
            }
        })?;
    }
    Ok(is_delegate)
}

/// The compile-time coherence checks, one bundle per leaf, each spanned at
/// the variant it indicts so the build error points at the declaration that
/// has to change:
///
/// - the leaf's `KINDS` is disjoint from the delegate's `CLAIMED_KINDS` and
///   from every earlier sibling's `KINDS` — one owner per stored string, or
///   the first-match parse chain shadows silently;
/// - every stored string the leaf's descriptors claim is in its `KINDS` —
///   the write path and the read path key off those two lists, and a string
///   in one but not the other is a row written but never resolved.
///
/// A const panic cannot format the colliding string into its message, so the
/// message names the colliding variants instead — the nearest place the
/// string is declared.
///
/// The bundle opens with a spanned trait assertion per variant, so a payload
/// missing one of the required impls fails AT the variant too — the generated
/// impls repeat the bound at the derive's own span, but the variant is where
/// the fix goes.
fn coherence_asserts(composition: &Composition<'_>) -> proc_macro2::TokenStream {
    let krate = &composition.krate;
    let delegate_ident = composition.delegate_ident;
    let delegate_ty = composition.delegate_ty;
    let mut asserts = proc_macro2::TokenStream::new();

    let delegate_span = delegate_ty.span();
    asserts.extend(quote_spanned! {delegate_span=>
        const _: () = {
            fn assert_delegate<__T>()
            where
                __T: #krate::agency::FromBlock
                    + #krate::agency::Agency
                    + #krate::agency::Projection,
            {
            }
            let _ = assert_delegate::<#delegate_ty>;
        };
    });

    for (index, leaf) in composition.leaves.iter().enumerate() {
        let ty = leaf.ty;
        let span = ty.span();

        asserts.extend(quote_spanned! {span=>
            const _: () = {
                fn assert_leaf<__T>()
                where
                    __T: #krate::agency::LeafKind
                        + #krate::agency::Agency
                        + #krate::agency::Projection,
                {
                }
                let _ = assert_leaf::<#ty>;
            };
        });

        let delegate_msg = format!(
            "variant `{}` claims a stored type string the delegate `{}` already claims; \
             every stored string has exactly one owner in the parse chain",
            leaf.ident, delegate_ident
        );
        asserts.extend(quote_spanned! {span=>
            const _: () = assert!(
                #krate::agency::kinds_disjoint(
                    <#ty as #krate::agency::LeafKind>::KINDS,
                    <#delegate_ty as #krate::agency::FromBlock>::CLAIMED_KINDS,
                ),
                #delegate_msg
            );
        });

        for earlier in &composition.leaves[..index] {
            let earlier_ty = earlier.ty;
            let sibling_msg = format!(
                "variants `{}` and `{}` both claim a stored type string; every stored \
                 string has exactly one owner in the parse chain",
                earlier.ident, leaf.ident
            );
            asserts.extend(quote_spanned! {span=>
                const _: () = assert!(
                    #krate::agency::kinds_disjoint(
                        <#ty as #krate::agency::LeafKind>::KINDS,
                        <#earlier_ty as #krate::agency::LeafKind>::KINDS,
                    ),
                    #sibling_msg
                );
            });
        }

        let coverage_msg = format!(
            "variant `{}` declares a descriptor claiming a stored type string its KINDS \
             does not list; the store would write rows the parse chain hands to the \
             inert fallback",
            leaf.ident
        );
        asserts.extend(quote_spanned! {span=>
            const _: () = assert!(
                #krate::agency::descriptor_kinds_claimed(
                    <#ty as #krate::agency::LeafKind>::DESCRIPTORS,
                    <#ty as #krate::agency::LeafKind>::KINDS,
                ),
                #coverage_msg
            );
        });
    }
    asserts
}

/// The `FromBlock` impl: the parse chain in declaration order, the
/// compile-time descriptor concatenation, and the claimed-kinds union that
/// makes nesting check transitively.
fn from_block_impl(name: &Ident, composition: &Composition<'_>) -> proc_macro2::TokenStream {
    let krate = &composition.krate;
    let delegate_ident = composition.delegate_ident;
    let delegate_ty = composition.delegate_ty;
    let leaf_ident: Vec<&Ident> = composition.leaves.iter().map(|leaf| leaf.ident).collect();
    let leaf_ty: Vec<&Type> = composition.leaves.iter().map(|leaf| leaf.ty).collect();
    quote! {
        #[automatically_derived]
        impl #krate::agency::FromBlock for #name {
            const DESCRIPTORS: &'static [#krate::store::ContentDescriptor] = {
                const SETS: &[&[#krate::store::ContentDescriptor]] = &[
                    <#delegate_ty as #krate::agency::FromBlock>::DESCRIPTORS,
                    #( <#leaf_ty as #krate::agency::LeafKind>::DESCRIPTORS, )*
                ];
                const CONCATENATED: [
                    #krate::store::ContentDescriptor;
                    #krate::store::descriptor_count(SETS)
                ] = #krate::store::concat_descriptors(SETS);
                &CONCATENATED
            };

            const CLAIMED_KINDS: &'static [&'static str] = {
                const SETS: &[&[&'static str]] = &[
                    <#delegate_ty as #krate::agency::FromBlock>::CLAIMED_KINDS,
                    #( <#leaf_ty as #krate::agency::LeafKind>::KINDS, )*
                ];
                const CONCATENATED: [&'static str; #krate::agency::kind_count(SETS)] =
                    #krate::agency::concat_kinds(SETS);
                &CONCATENATED
            };

            fn from_block(block: &#krate::Block) -> Self {
                #(
                    if <#leaf_ty as #krate::agency::LeafKind>::KINDS
                        .contains(&block.block_type.as_str())
                    {
                        return Self::#leaf_ident(
                            <#leaf_ty as #krate::agency::LeafKind>::parse(block),
                        );
                    }
                )*
                Self::#delegate_ident(
                    <#delegate_ty as #krate::agency::FromBlock>::from_block(block),
                )
            }
        }
    }
}

/// The `Agency` impl: per-variant delegation on every hook — `durable`
/// included, because a row's lifetime is the leaf's own answer like
/// everything else.
fn agency_impl(name: &Ident, composition: &Composition<'_>) -> proc_macro2::TokenStream {
    let krate = &composition.krate;
    let all_ident = composition.all_idents();
    quote! {
        #[automatically_derived]
        impl #krate::agency::Agency for #name {
            fn awaiting(&self) -> ::core::option::Option<#krate::Awaiting> {
                match self {
                    #( Self::#all_ident(kind) => #krate::agency::Agency::awaiting(kind), )*
                }
            }

            fn durable(&self) -> bool {
                match self {
                    #( Self::#all_ident(kind) => #krate::agency::Agency::durable(kind), )*
                }
            }

            async fn gate<__E: #krate::RuntimeEvent>(
                &self,
                ctx: &#krate::AgencyCtx<__E>,
            ) -> #krate::GateDecision {
                match self {
                    #( Self::#all_ident(kind) =>
                        #krate::agency::Agency::gate(kind, ctx).await, )*
                }
            }

            async fn run<__E: #krate::RuntimeEvent>(
                &self,
                ctx: &#krate::AgencyCtx<__E>,
            ) -> ::core::result::Result<bool, #krate::StoreError> {
                match self {
                    #( Self::#all_ident(kind) =>
                        #krate::agency::Agency::run(kind, ctx).await, )*
                }
            }

            fn post_gate_id(
                &self,
                ledger: &[#krate::Block],
            ) -> ::core::option::Option<i64> {
                match self {
                    #( Self::#all_ident(kind) =>
                        #krate::agency::Agency::post_gate_id(kind, ledger), )*
                }
            }

            async fn run_post_gate<__E: #krate::RuntimeEvent>(
                &self,
                ctx: &#krate::AgencyCtx<__E>,
            ) -> ::core::result::Result<(), #krate::StoreError> {
                match self {
                    #( Self::#all_ident(kind) =>
                        #krate::agency::Agency::run_post_gate(kind, ctx).await, )*
                }
            }
        }
    }
}

/// The `Projection` impl: per-variant delegation on all four methods.
fn projection_impl(name: &Ident, composition: &Composition<'_>) -> proc_macro2::TokenStream {
    let krate = &composition.krate;
    let all_ident = composition.all_idents();
    quote! {
        #[automatically_derived]
        impl #krate::agency::Projection for #name {
            fn group_role(&self) -> ::core::option::Option<#krate::Role> {
                match self {
                    #( Self::#all_ident(kind) =>
                        #krate::agency::Projection::group_role(kind), )*
                }
            }

            fn llm_parts(
                &self,
            ) -> ::core::option::Option<::std::vec::Vec<#krate::ContentPart>> {
                match self {
                    #( Self::#all_ident(kind) =>
                        #krate::agency::Projection::llm_parts(kind), )*
                }
            }

            fn llm_text(&self) -> ::core::option::Option<::std::string::String> {
                match self {
                    #( Self::#all_ident(kind) =>
                        #krate::agency::Projection::llm_text(kind), )*
                }
            }

            fn forces_parts(&self) -> bool {
                match self {
                    #( Self::#all_ident(kind) =>
                        #krate::agency::Projection::forces_parts(kind), )*
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::expand;

    /// The enum every test expands: shape-valid, types unresolved — the
    /// expansion is inspected as tokens, never compiled here.
    fn composed(with_crate_override: bool) -> syn::DeriveInput {
        if with_crate_override {
            syn::parse_quote! {
                #[agency(crate = the_ledger)]
                enum Composed {
                    #[agency(delegate)]
                    Core(Inner),
                    Leaf(LeafTy),
                }
            }
        } else {
            syn::parse_quote! {
                enum Composed {
                    #[agency(delegate)]
                    Core(Inner),
                    Leaf(LeafTy),
                }
            }
        }
    }

    /// Without an override, every generated path reaches `::agent_ledger`.
    #[test]
    fn the_default_crate_path_is_the_library() {
        let generated = expand(&composed(false))
            .expect("a valid composition expands")
            .to_string();
        assert!(
            generated.contains("agent_ledger"),
            "the default path names the library: {generated}"
        );
        assert!(
            !generated.contains("the_ledger"),
            "no override path appears without the attribute: {generated}"
        );
    }

    /// With `#[agency(crate = the_ledger)]`, NO generated path names
    /// `agent_ledger` — the override threads through the impls, the parse
    /// chain, the descriptor concatenation and every coherence assert, so a
    /// consumer holding the library under a renamed dependency is not locked
    /// out by a single hardcoded path anywhere.
    #[test]
    fn the_crate_override_threads_through_every_generated_path() {
        let generated = expand(&composed(true))
            .expect("a valid composition expands")
            .to_string();
        assert!(
            !generated.contains("agent_ledger"),
            "a generated path escaped the crate override: {generated}"
        );
        assert!(
            generated.contains("the_ledger"),
            "the override path is what the generated code resolves through: {generated}"
        );
    }
}
