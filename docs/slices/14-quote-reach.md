# Slice 14 — a quote can reach any kind that declares its text

Date: 2026-08-28. The quote mechanism is whole on its own kinds and blind to everyone
else's. A `quote` block stores a span reference and resolves it to text at store-read time
(`store/blocks.rs:517-530`, `resolve_quote_text` at `:558-599`), projecting as `> `-prefixed
lines with an empty resolution rendering empty rather than as a bare marker
(`agency/projection.rs:137-148`). But the resolver reads **`block_text` alone**
(`blocks.rs:570`, and `quoted_text_blocks` for ranges), so a quote referencing a consumer's
kind — the very messages a chat consumer holds — resolves to the empty string. A consumer
cannot quote its own conversation.

The consumer that surfaced this needs inbound platform replies to land as quotes of the
replied-to chat message (its unit 31). That unit is unbuildable until the resolver can see
consumer text.

## Grounding

**The resolver's two paths, both `block_text`-only.** Single-block: one `SELECT` from
`block_text`, then a **character**-offset slice via `chars().skip(s).take(e-s)`
(`blocks.rs:566-581`) — positions are char counts, not bytes, so no UTF-8 boundary hazard
exists in the current shape and none may be introduced. Range: `quoted_text_blocks` walks
the junction-visible span (`:584-599`), with the membership rule documented at `:550-557`
— the range covers only what the quoting conversation can see, because block ids are
global.

**Consumer kinds already declare their storage.** A `ContentDescriptor` names its table,
domain, kinds and columns (`store/descriptors.rs:147-163`), and every descriptor read maps
declared columns into `block.fields`. What no descriptor declares is which column, if any,
is the kind's quotable text.

**Both quote sites share the resolution.** `resolve_quotes` in the block loader
(`blocks.rs:510-530`) and the fork's deep copy (`store/block_content.rs:74`) resolve
through the same functions; a widened resolver serves both without a second decision.

**Erasure semantics come free and must stay free.** The consumer nulls a chat message's
text column on erasure; a resolver that reads the declared column through `COALESCE`
resolves an erased message to the empty string, and the projection renders an empty quote
as nothing. No erasure special case may appear in the resolver.

## Decisions taken with this slice

- **A descriptor may declare its quotable text column, 2026-08-28.** `ContentDescriptor`
  gains `quoted_text_column: Option<&'static str>`, default `None`, naming one of the
  descriptor's own declared columns. `None` means the kind's content cannot be quoted and
  resolves as empty — exactly the today behaviour, stated instead of accidental.
  *Rejected:* resolving through the kind's projection (`llm_text`) — resolution runs at
  store-read, before kinds are parsed, and the store must not construct agency types;
  *rejected:* mirroring consumer text into `block_text` (two copies of a member's message,
  and the copy erasure does not know about).
- **The resolver consults the descriptor when `block_text` has nothing, 2026-08-28.** For
  each block in the span: read `block_text` as today; where the block's row is absent
  there, look up its stored type's descriptor and, where one declares a quotable column,
  read that column under the descriptor's domain, `COALESCE`d to empty. Character
  offsets apply unchanged. The membership rule (`quoted_text_blocks`) stays the single
  gate for ranges. *Rejected:* a union view over all text-bearing tables (a schema object
  encoding a fact the descriptors already state); *rejected:* widening only the
  single-block path (the range path would silently skip consumer blocks — one rule, two
  behaviours).
- **The declaration is validated at open, 2026-08-28.** A `quoted_text_column` naming a
  column the descriptor does not declare, or one whose type is not text, is refused at
  descriptor open exactly as the header-shadowing names are (`descriptors.rs:144-146`) —
  a wrong declaration fails loudly at startup, never quietly at quote time.
- **No behaviour change for any existing store or kind, 2026-08-28.** Every framework kind
  keeps resolving through `block_text`; every consumer kind without the declaration keeps
  resolving empty; no schema changes, no migration — the field is compile-time descriptor
  data.

## The slice's contract

A consumer kind whose descriptor names a quotable text column is readable by the quote
resolver: a quote block spanning it resolves that column's text with the same character
offsets, the same conversation-membership rule and the same empty-on-absence behaviour the
framework's own text enjoys, at both quote sites. A kind without the declaration resolves
empty, as today. A declaration naming an undeclared or non-text column is refused at open.
An erased row whose text column is null resolves empty with no special case. No migration,
no new dependency, no change to any existing render pin.

## Acceptance criteria

- **AC1** Workspace suite green with and without every provider feature; clippy, fmt, doc
  under denied warnings; no new dependency.
- **AC2** A quote of a consumer kind resolves: a test descriptor with a declared quotable
  column, a block of that kind, and a quote block spanning a character range of it renders
  the sliced text through the real projection fold — pinned, and failing on the pre-change
  code.
- **AC3** Offsets are characters at the seam: the pinned span includes multibyte
  characters and slices between them without panic or truncation mid-character.
- **AC4** The membership rule holds for consumer blocks: a range quote in one conversation
  does not pick up another conversation's consumer block appended between the endpoints —
  pinned, since this is the documented hazard of global block ids.
- **AC5** Absence stays empty: a kind with no declaration, an erased row (text column
  null), and a dangling reference each resolve to the empty string and render as nothing —
  pinned per case.
- **AC6** A bad declaration refuses at open: an undeclared column name and a non-text
  column each fail descriptor open with a named error — pinned.
- **AC7** Both sites resolve identically: the loader and the fork deep-copy path produce
  the same text for the same quote — pinned through the fork.
- **AC8** Nothing that ships changes: every existing quote, render and fork pin passes
  unedited.

## Notes for launch

- Branches from `master` (worktree `~/projects/agent-ledger-quote-reach`, branch
  `unit/quote-reach`). Sites: `store/descriptors.rs` (the field, open-time validation),
  `store/blocks.rs` (`resolve_quote_text`, `quoted_text_blocks`),
  `store/block_content.rs` (the fork site), and tests beside each.
- The consumer's declaration (its chat message naming its text column) belongs to consumer
  unit 31, not here — this slice ships with a test descriptor only.
- Slices 12 (async projection) and 13 (dated appends) live on unmerged branches; this
  slice touches none of their files except possibly test call shapes. Whichever lands
  later rebases mechanically.
