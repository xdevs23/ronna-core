# Slice 14 — a quote can reach any kind that declares its text

Date: 2026-08-28; revised 2026-08-29 against a cold round that measured the tree after
slices 13 and 15 merged. The quote mechanism is whole on its own kinds and blind to
everyone else's. A `quote` block stores a span reference and resolves it to text at
store-read time (`resolve_quotes` at `store/blocks.rs:528-549`, `resolve_quote_text` at
`:575-616`), projecting as `> `-prefixed lines with an empty resolution rendering empty
rather than as a bare marker (`agency/projection.rs:137-148`). But the resolver reads
**`block_text` alone** (the SELECT at `blocks.rs:585-591`, and `quoted_text_blocks` at
`:655-690` for ranges), so a quote referencing a consumer's kind — the very messages a
chat consumer holds — resolves to the empty string. A consumer cannot quote its own
conversation.

The consumer that surfaced this needs inbound platform replies to land as quotes of the
replied-to chat message (its unit 31). That unit is unbuildable until the resolver can
see consumer text.

## Grounding

**The resolver's two paths, both `block_text`-only.** Single-block: one `SELECT` from
`block_text`, then a **character**-offset slice via `chars().skip(s).take(e-s)`
(`blocks.rs:583-598`) — positions are char counts, not bytes, so no UTF-8 boundary
hazard exists in the current shape and none may be introduced. Range:
`quoted_text_blocks` walks the junction-visible span (`:655-690`), with the membership
rule documented at `:564-574` and `:618-654` — the range covers only what the quoting
conversation can see, because block ids are global. A date-marker row inside a span
contributes nothing after this slice for a stated reason: no descriptor can ever claim
the `date_marker` kind (the core-kinds collision refusal,
`store/descriptors.rs:521-528`), so the widened walk's descriptor lookup misses it —
pinned by this slice so the fork consequence below can rely on it.

**Three callers share the resolution.** The block loader (`blocks.rs:528`), the drafts
path (`store/drafts.rs:171` — a composer previewing a quote), and the fork's deep copy
(`collect_quote_targets` at `store/conversations.rs:771-804`, feeding
`deep_copy_group_into` at `:819`). A widened resolver serves all three without a second
decision, and each gets its pin.

**Consumer kinds already declare their storage.** A `ContentDescriptor` names its
table, domain, kinds, columns, reference columns and its ephemeral flag
(`store/descriptors.rs:148-183`), and every descriptor read maps declared columns into
`block.fields` (`descriptors.rs:890-903`). What no descriptor declares is which column,
if any, is the kind's quotable text.

**The store's gate discipline binds descriptor reads.** Every descriptor-path read
consults the domain gate and refuses on a failed consumer migration (the invariant on
`DomainGate`, `store/mod.rs:493-506`; the precedent at `descriptors.rs:850`) — but the
resolver's functions return bare `String`/`Vec`, not `Result`. The slice must take a
side, and does, below.

**Erasure semantics come free and must stay free — and the passes are the
consumer's, not this library's.** The consumer nulls a chat message's text column on
erasure; a resolver that reads the declared column through `COALESCE` resolves an
erased message to the empty string, and the projection renders an empty quote as
nothing. No erasure special case may appear in the resolver. The FORK is the one
place a real copy appears: a fork whose quoted range spans a consumer block
deep-copies the row into the consumer's own content table (`clone_consumer_content`,
`descriptors.rs:1197`), copying EVERY declared column — so whatever principal or
origin columns the consumer declares ride to the clone, and the consumer's own
erasure passes (which live in the consumer repository, not here) reach the clone
exactly as they reach any row of that table. This slice pins the copy's completeness;
the erasure-parity pin over clones belongs to consumer unit 31, stated so neither
side assumes the other did it.

## Decisions taken with this slice

- **A descriptor may declare its quotable text column, 2026-08-28.** `ContentDescriptor`
  gains `quoted_text_column: Option<&'static str>`, default `None`, naming one of the
  descriptor's own declared columns. `None` means the kind's content cannot be quoted and
  resolves as empty — exactly the today behaviour, stated instead of accidental.
  *Rejected:* resolving through the kind's projection (`llm_text`) — resolution runs at
  store-read, before kinds are parsed, and the store must not construct agency types;
  *rejected:* mirroring consumer text into `block_text` (two copies of a member's message,
  and the copy erasure does not know about).
- **Membership follows the DECLARATION; text follows the gate, 2026-08-29.** The
  span walk's membership becomes: a `block_text` row, OR a stored type whose
  descriptor declares a quotable column — a static, compile-time fact, so what a
  span covers never depends on runtime state. A kind with no declaration is NOT a
  member: exactly today's walk, so forks copy exactly today's set for undeclared
  kinds. A declared kind IS a member even when its domain gate is closed — its text
  resolves empty (below) and a fork's clone of it writes through the gate like every
  consumer write, failing the fork loudly if the schema is in doubt, the store's
  standing discipline. Recorded because the fork consumes the same walk
  (`collect_quote_targets`), so membership here IS the fork's copy set, not a text
  detail.
- **The resolver consults the descriptor when `block_text` has nothing, 2026-08-28.** For
  each member of the span: read `block_text` as today; where the block's row is absent
  there, read the declared quotable column under the descriptor's domain, `COALESCE`d
  to empty. Character offsets apply unchanged; the two text sources interleave in
  ledger id order, the walk's own order. The membership rule (`quoted_text_blocks`)
  stays the single gate for ranges. *Rejected:* a union view over all text-bearing tables (a schema object
  encoding a fact the descriptors already state); *rejected:* widening only the
  single-block path (the range path would silently skip consumer blocks — one rule, two
  behaviours).
- **A closed domain gate resolves empty without touching the table, 2026-08-29.** When
  the descriptor's domain gate is closed — a failed consumer migration — the resolver
  does not read the column and the quote resolves empty, joining the absence list. The
  decline IS the gate discipline: nothing runs raw against a schema in doubt, and a
  quote is display enrichment, not a fact worth failing a whole conversation load
  over. *Rejected:* rethreading the resolver's signatures to `Result` (a failed
  consumer migration would fail every load of any conversation holding a
  consumer-spanning quote — an error-surface widening nothing asked for);
  *rejected:* reading anyway (the exact raw-read the gate exists to refuse).
- **The declaration is validated at open, and two lazy declarations are refused with
  it, 2026-08-28; edges settled 2026-08-29.** A `quoted_text_column` naming a column
  the descriptor does not declare, or one whose type is not text, is refused at
  descriptor open in `validate_columns` (`descriptors.rs:540-556`, the
  `RESERVED_COLUMNS` precedent) — and so are a declaration naming the ROLE column (a
  quote resolving to the literal string "user" is a wrong declaration, not a
  behaviour) and one on an EPHEMERAL kind (finalization deletes its rows, so every
  quote of it would dangle by design). A wrong declaration fails loudly at startup,
  never quietly at quote time.
- **No behaviour change for any existing store or kind, 2026-08-28.** Every framework
  kind keeps resolving through `block_text`; every consumer kind without the
  declaration keeps resolving empty; no schema changes, no migration — the field is
  compile-time descriptor data. Existing struct literals of `ContentDescriptor` (the
  consumer-kind test suite, the descriptor tests, the derive crate's doc example at
  `agent-ledger-derive/src/lib.rs:246`) gain the field mechanically; the criterion
  below binds their BEHAVIOUR, not their bytes.

## The slice's contract

A consumer kind whose descriptor names a quotable text column is readable by the quote
resolver: a quote block spanning it resolves that column's text with the same character
offsets, the range walk's conversation-membership rule and the same
empty-on-absence behaviour the framework's own text enjoys, at all three call sites
(the single-block path stays membership-free, exactly as it is for framework text
today). A kind without the
declaration resolves empty, as today; so does a span whose descriptor's domain gate is
closed, without a raw read. A declaration naming an undeclared, non-text, role, or
ephemeral-kind column is refused at open. An erased row whose text column is null
resolves empty with no special case; a fork's deep copy of a quoted consumer block
lands in the consumer's own table where person-keyed erasure already walks. No
migration, no new dependency, no change to any existing render pin's meaning.

## Acceptance criteria

- **AC1** Workspace suite green with and without every provider feature; clippy, fmt,
  doc under denied warnings (the derive doc example compiles with the new field); no
  new dependency.
- **AC2** A quote of a consumer kind resolves: a test descriptor with a declared
  quotable column, a block of that kind, and a quote block spanning a character range
  of it renders the sliced text through the real projection fold — pinned, and failing
  on the pre-change code.
- **AC3** Offsets are characters at the seam: the pinned span includes multibyte
  characters and slices between them without panic or truncation mid-character.
- **AC4** The membership rule holds for consumer blocks: a range quote in one
  conversation does not pick up another conversation's consumer block appended between
  the endpoints — pinned; and a date-marker row inside a quoted span contributes
  nothing — pinned.
- **AC5** Absence stays empty, four ways: a kind with no declaration, an erased row
  (text column null), a dangling reference, and a CLOSED domain gate each resolve to
  the empty string and render as nothing — pinned per case. The closed-gate pin's
  no-raw-read proof: close the gate BEFORE the consumer table exists, so any raw
  read would error — an empty resolution with no error is the proof, no query
  tracing needed.
- **AC6** A bad declaration refuses at open: an undeclared column, a non-text column,
  the role column, and a column on an ephemeral kind each fail descriptor open with a
  named error — pinned per case.
- **AC7** All three sites resolve identically: the loader, the drafts preview, and the
  fork path — the fork pinned by forking a conversation whose quote spans a consumer
  block, loading the destination, and comparing resolutions; the same pin proves the
  clone landed in the consumer's table with EVERY declared column copied (the
  consumer's erasure reach over clones is unit 31's pin, not this one's).
- **AC8** No shipped behaviour changes: every existing quote, render and fork pin
  passes with its assertions' meanings intact — the mechanical field addition to
  descriptor literals is the one permitted edit, and no expected value moves.

## Notes for launch

- Branches from `master` (worktree `~/projects/agent-ledger-quote-reach`, branch
  `unit/quote-reach`, rebased 2026-08-29 onto a2147fe — a master that already carries
  slices 13 and 15; only slice 12, async projection, remains unmerged). Sites:
  `store/descriptors.rs` (the field, open-time validation in `validate_columns`),
  `store/blocks.rs` (`resolve_quote_text`, `quoted_text_blocks`),
  `store/conversations.rs` (`collect_quote_targets` / `deep_copy_group_into`, the
  fork path), `store/drafts.rs:171` (the preview caller — its move-closure captures
  the descriptors and the gate), the resolver signatures themselves (they take only
  `conn` today; the descriptor set and the gate thread through the three callers —
  a mechanical ripple, and NOT the Result-rethreading the gate decision rejects),
  the derive crate's doc example, and tests beside each.
- The consumer's declaration (its chat message naming its text column) belongs to
  consumer unit 31, not here — this slice ships with a test descriptor only.
