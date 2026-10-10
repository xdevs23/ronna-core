# Slice 24: the Anthropic wire reports failures, refusals and effort correctly

Date: 2026-10-11.

The Anthropic provider has so far run only against fixtures. Reading it
against the Messages API documentation shows four places where the wire
says something different from what the API means. Each of them turns a
failure into silence or sends a field where the API does not read it.

## What is wrong today

1. **An error event inside the stream is ignored.** The API can answer a
   streamed request with HTTP 200 and then send an SSE event of type
   `error`, for example `{"type": "error", "error": {"type":
   "overloaded_error", "message": "Overloaded"}}`. The event parser
   matches only the content block events, `message_start` and
   `message_delta`, and drops every other type. The stream then ends, the
   pump reports a finished stream, and the bind loop closes the turn
   without an end-of-turn event and without an error. The reader commits
   whatever text had streamed as if it were the whole answer, and nothing
   records that the server failed.
2. **An overloaded or failing server ends the turn.** A response with
   status 529 (`overloaded_error`) or 500 (`api_error`) becomes
   `LlmError::Api`, which `LlmError::is_recoverable` classifies as
   terminal for every status. The Anthropic error reference lists 500 and
   529 as retryable, and its SDKs retry every status of 500 and above.
   The bind loop already has a reconnect path with backoff, but a server
   error is never sent down it.
3. **A refusal reads as a normal end of turn.** The stop reason mapping
   knows `tool_use` and `max_tokens` and maps everything else to
   `EndTurn`. A `refusal` stop, which the API sends with HTTP 200 when a
   safety classifier or the model declines, therefore ends the turn like a
   success, often with empty content. A `max_tokens` stop, by contrast,
   records an error status block and closes the turn with the stream
   error `max_tokens`. A refusal can also arrive mid-stream after partial
   output, and the API documentation asks for that partial output to be
   discarded. Any other stop reason the wire does not know, such as
   `model_context_window_exceeded`, also passes for a normal end of turn.
4. **The reasoning effort is sent where the API does not read it.** A
   reasoning level sets a top-level `effort` field. The Messages API
   expects the effort inside `output_config`, as `output_config.effort`,
   beside `output_config.format` when a response schema is also set.

## The change

1. **In-stream errors become typed stream errors.** The Anthropic event
   parser reads an `error` event and turns its error type into the
   neutral error that the bind loop already classifies:
   - `overloaded_error` and `api_error` become the new
     `LlmError::Unavailable`, which is recoverable. It contains the error
     type and message from the provider and no HTTP status, because the
     request itself succeeded.
   - `rate_limit_error` becomes `LlmError::RateLimited` without a wait
     hint, so the existing rate-limit path handles it.
   - Every other type, and an error event without a type, becomes
     `LlmError::ProviderFailure`, which is still terminal. A failure that
     cannot be classified is never retried.
2. **Server errors are recoverable.** `LlmError::is_recoverable` returns
   true for `LlmError::Api` with a status of 500 or above, and for
   `LlmError::Unavailable`. A 4xx response is still terminal. A
   recoverable error takes the existing reconnect path of the bind loop:
   a restart of the turn, the existing backoff and the existing limit on
   attempts.
3. **A refusal is its own stop reason.** `StopReason` gains `Refusal`, the
   Anthropic wire maps `refusal` to it, and the reader treats it like the
   other abnormal stops: it records an error status block and closes the
   turn with the stream error `refusal`, so the conversation latches and a
   retry resumes from the turn that is still owed. Unlike the other
   abnormal stops, a refusal discards the output of the turn that is not
   committed yet: the streamed text tail is deleted instead of finalized,
   and the reader ingests nothing more of the turn, so tool calls that a
   vendor sends after the end of the turn are not recorded. Whether a stop
   discards that output and which stream error it closes with are defined
   on `StopReason`.
4. **An unknown stop reason fails the turn.** The Anthropic wire maps
   `end_turn`, `tool_use`, `max_tokens` and `refusal`. Any other stop
   reason, and a `message_delta` without one, becomes a terminal
   `LlmError::ProviderFailure` that names it, so an unknown stop never
   passes for a finished answer.
5. **Effort moves into `output_config`.** The Anthropic request contains
   `output_config` with an optional `effort` and an optional `format`.
   It is sent when either is set and omitted when neither is.

### Effect on the other providers

The recoverable classification is part of the neutral error type, so every
vendor on the bind loop gains it: the chat completions dialect and the
vendors built on it, the Responses dialect, and Kimi. Each of them reports
a non-success HTTP response as `LlmError::Api` with its status, so a 5xx
from any of them now reconnects instead of ending the turn. A 5xx means the
server failed, whichever vendor sent it.

The payload-omitting fallback is affected in one case. A 5xx from an
attempt that replayed reasoning payloads now reconnects with the payloads
still in place, and the fallback runs only after a later terminal error.
The fallback exists for a request that the provider rejects, which it
reports as a 4xx or as an in-stream verdict, and both of those are still
terminal.

The in-stream verdicts of the other vendors are unchanged. The Responses
dialect still reports a failed or cancelled response as
`LlmError::ProviderFailure`.

## What does not change

- The reconnect path itself: its backoff, its limit on attempts, its
  status line and the restart that discards the uncommitted blocks of the
  turn.
- The rate-limit path and its budget.
- A 4xx response, which is terminal.
- The `max_tokens` and `content_filter` stops.
- The request body when no reasoning level and no response schema are
  set, which has no `output_config`.
- Prompt caching, which this change does not add.

## Acceptance criteria

1. An `error` event of type `overloaded_error` or `api_error` parses to a
   recoverable `LlmError::Unavailable` that contains the type and message.
2. An `error` event of type `rate_limit_error` parses to
   `LlmError::RateLimited` without a wait hint.
3. An `error` event of any other type, or without a type, parses to a
   terminal `LlmError::ProviderFailure`.
4. `LlmError::Api` with status 500, 503 or 529 is recoverable, and with
   status 400, 404 or 429 it is terminal.
5. A `message_delta` with stop reason `refusal` parses to an end of turn
   with `StopReason::Refusal`.
6. A turn that ends with `StopReason::Refusal` records one error status
   block and closes with exactly one stream error, `refusal`. Text
   streamed before the refusal and tool calls sent after it leave no
   block behind.
7. A `message_delta` with an unknown stop reason, or without one, parses
   to a terminal `LlmError::ProviderFailure`.
8. A request with a reasoning level sends `output_config.effort` and no
   top-level `effort`. A request with a reasoning level and a response
   schema sends one `output_config` with both `effort` and `format`. A
   request with neither sends no `output_config`.
9. `cargo test --workspace --all-features`, `cargo clippy --workspace
   --all-features --all-targets -- -D warnings` and `cargo fmt --all
   --check` are clean.
