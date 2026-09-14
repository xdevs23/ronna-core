# Slice 21 — a response schema passed through to the model

Date: 2026-09-13, the date the design was decided. Written after the first
implementation commit, from the words that preceded it; the criteria below
are what that commit was reviewed against.

## The design

A consumer can hand a turn a JSON Schema. The provider holds the model to
it and the consumer reads the answer back as JSON. That is the whole
feature: the framework passes the schema through to the API and returns
the deserialized object. It validates nothing locally, offers no fallback,
runs no retry without the schema, and keeps no allowlist of models.

A previous attempt built a block kind for the schema, a second block kind
for the answer, local validation of both, and a rewrite of a streaming
parser. None of that is part of this design.

## The mechanism

1. **The schema rides the tool-choice record.** The record already states
   what a turn is offered; it now also states what shape the turn answers
   in, as an optional column beside the names. The schema has exactly the
   lifetime the names have: the newest record speaks for both columns at
   once, so an ordinary tool-choice append, which records no schema, clears
   one, and a compacted thread that copies the record copies the schema with
   it. A stored column that is not JSON is a corrupt row to both readers of
   the record, like a names column that is not a list.
2. **A temporary fork takes the schema** and writes it on the empty tool
   choice it already records ahead of the instructions. A compacted thread
   copies its source's newest record verbatim, schema included.
3. **The actor folds the record once** for the tools and the schema and
   sends both with the request. The bind loop's opener receives one
   `TurnRequest` value carrying messages, model, tools, reasoning and the
   schema; every re-open of the same turn receives the same value, and every
   vendor converts it into its neutral request through one conversion. This
   changes the opener's public signature. The library is at version 0.1 with
   one consumer, which is updated in the same step; no compatibility wrapper
   is kept.
4. **Each dialect puts the schema where its API expects one.** The chat
   completions dialect and Kimi send `response_format` of type
   `json_schema` with a fixed name, `strict: true` and the schema; the
   Responses dialect sends `text.format` with the same fields; the
   Anthropic dialect sends `output_config.format` with type `json_schema`
   and the schema. A request without a schema serializes exactly as before.
5. **The answer is read back through the temporary conversation.**
   `TemporaryConversation::answer_text` is the newest non-empty assistant
   text past the instructions block; `answer::<T>` deserializes it.
   Nothing written is `None`; text that is not the requested shape is an
   error the caller reports, never a shape repaired here.

## Acceptance criteria

1. `CompletionRequest` and `ProviderRequest::Stream` carry an optional
   schema, and a request without one produces the same wire body as before
   on every dialect.
2. Each of the four dialect envelopes is asserted by a golden test with the
   caller's schema document unchanged inside it, and every provider feature
   builds on its own.
3. The tool-choice record stores the schema; both readers of the record
   (the ledger fold and the one-statement read) answer the same schema; an
   ordinary append clears it; a column that is not JSON is refused by both
   readers.
4. Migration v10 adds the column in place; every choice recorded before it
   reads as no schema.
5. A temporary fork's schema reaches the dispatch its own empty choice
   governs, read from the same snapshot as the tools, folded once.
6. The bind loop hands the same schema to every re-open of one turn.
7. The compacted thread copies the source's newest record verbatim,
   schema included.
8. `answer_text` reads only past the instructions, answers `None` for a
   slice that does not carry the instructions block, and skips whitespace;
   `answer::<T>` returns the typed value, `None` for nothing written, and
   an error for text that does not deserialize as `T`.
9. No local schema validation, no fallback to prose, no retry without the
   schema, and no model allowlist exist anywhere in the change.
10. `cargo test --all-features`, `cargo clippy --all-features --all-targets`
    and `cargo fmt --all --check` are clean.
