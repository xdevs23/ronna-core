# Slice 8 — the consumer follow-ups this slice unblocks

Date: 2026-08-22. Slice 8 (dispatch identity) ships the dispatch anchor and the
reworked turn-close edge. The sibling consumer — developed against this library
by sibling checkout, per its own decision record on the dependency — carries
these follow-ups, which were waiting on exactly these facts. They are one
coordinated unit on the consumer's side, not drive-by edits, because the
second and third only hold once the first is in place. Item 6 lists the
reserved-name breaks the slice introduces beside the field itself.

1. **The trailing done on its scripted providers.** The turn now closes on the
   stream's closed signal, which the reader emits at the provider's `Done` —
   real wires already send it after every completed turn, and this repository's
   scripted providers gained it as enumerated edits in this slice. The
   consumer's scripted providers need the same trailing `Done` after each
   completed turn's events, or its suites stall on a dispatch state that never
   closes.
2. **The duplicate-turn canary un-ignores.** The consumer's tools suite carries
   an ignored canary test for the duplicate-turn redispatch, written to flip
   when the framework fix ships. It has shipped: a message appended in the held
   post-message-end window no longer dispatches a second turn, and the close's
   re-check dispatches the absorbed message's turn deterministically — pinned
   here with real interleaving.
3. **The echo tolerance comes out.** The consumer's tool-scripted provider
   answers a repeated call round as an empty turn and counts it, so the defect
   could not skew its suites. With the defect closed, the tolerance is dead
   code that would mask a regression; it and its `echoes` counter come out.
4. **The block-struct field.** `Block` gained `dispatch_anchor: Option<i64>` —
   the compile break this slice's design accepted and recorded. Every consumer
   `Block { .. }` literal gains the field; loads through the store carry it
   automatically.
5. **The registration floor lifts.** The consumer floored tool registration at
   its lowest authority because nothing recorded which message summoned a
   turn, and reconstructing it from stored shape was adversarially refuted
   (its decision record closing that question is the citation for the
   "derived, never stored" exception in this repository's invariants). The
   fact now exists: from a tool-call block, one `find_block` on the call's
   anchor loads the summoning frontier, in round one and in a continuation
   round alike. For a turn summoned by a message, that frontier IS the
   message; a null anchor is the documented out-of-band answer to fold to the
   floor; and a turn whose dispatching frontier is itself a null-anchored
   turn product — the out-of-band path's tool result — anchors on that result
   block, not on a message, so the gate's fold decides that non-message shape
   beside the null one.

   **The recommended gate form, for every cross-author shape:** read the
   anchor as the LOWER BOUND of the turn's authors, never as the single
   author. The dispatched request is built from the ledger up to the call, so
   the sound admission is the minimum-authority fold over the interval from
   the summoner to the call, `[summoner..call]`: fold in every author whose
   blocks lie inside it, admit at the minimum. With the anchor as the lower
   bound the fold is well-defined and it covers the inherited shapes by
   construction. The instructive one: a message absorbed while a turn is
   open is answered by the turn the close dispatches, whose frontier is the
   open turn's own tool result — its calls anchor on the PREVIOUS summoning
   message even though the request carries the absorbed text. The anchor
   names the turn's dispatch identity, never the newest author in the
   request; the fold still decides the shape correctly, because the absorbed
   message lies inside `[summoner..call]` and its author is folded in. With
   that reading the floor lifts into the intended minimum-authority
   provenance gate: null folds to the floor, a non-message frontier is one
   more author the interval covers.
6. **The reserved-name breaks, by name.** Beside the struct field, the slice
   reserves the name `dispatch_anchor` on both consumer-facing surfaces:
   - `RESERVED_FIELD_NAMES` (the block serializer's header-owned keys) now
     contains `dispatch_anchor`. A consumer block kind whose payload carries a
     field of that name has it DROPPED from the serialized form with a logged
     warning — the row header wins, so a payload cannot forge a turn identity
     the header never recorded. Rename the payload field.
   - `RESERVED_COLUMNS` (the content-table descriptor validator's list) now
     contains `dispatch_anchor`. A consumer descriptor declaring a column of
     that name is REFUSED AT STORE OPEN, at runtime: the store's open fails
     with the descriptor-validation error before any query is served, so the
     consumer's process does not start until the column is renamed. This is
     the sharp edge of the two — it is an open-refusal, not a silent drop.
