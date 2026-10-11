# Slice 26: a consumer can read from a stream error whether a retry can succeed

Date: 2026-10-11.

A program that drives a conversation without a person, for example one
that resends the turn after a failed round, needs to know whether a resend
can succeed. The runtime already knows this at every place that ends a
turn with an error, and drops it before the error reaches the consumer.

## What is wrong today

1. **The typed failure becomes text before it leaves the bind loop.** The
   bind loop retries a recoverable failure itself
   (`LlmError::is_recoverable`): it reconnects a dropped or idle stream and
   a server error, up to its limit on attempts, and it waits out a rate
   limit within its own budget. When it gives up, or when the failure is
   terminal, `close_turn` sends `ProviderResponse::Error(String)` with the
   rendered error, and the ingestion reader emits
   `CoreEvent::StreamError { error: String, .. }` from it. The `LlmError`
   is gone at that point.
2. **An abnormal stop uses the same field.** A turn that ends with
   `max_tokens`, `content_filter` or `refusal` closes with a
   `StreamError` whose `error` is the key from `StopReason::error_key`.
3. **The actor's own failures use it too.** A failed draft promotion, a
   failed insert of user blocks, a failed provider bind and the refusal to
   dispatch over a ledger that does not open with its head kind each emit
   a `StreamError` with `generation: None` and the error as text.
4. **A consumer cannot tell the two kinds of failure apart.** An overload,
   a server error or a lost connection that outlasted the reconnects of
   the bind loop, or a rate limit that outlasted its budget, may pass on a
   later resend of the same turn. A 4xx such as a wrong key or a rejected
   request, a missing key, a configuration error, a parse failure, an
   unknown stop reason or a refusal fails the same way on every resend. A
   consumer can only compare the error text or resend everything until a
   deadline of its own.

## The change

1. **`CoreEvent::StreamError` gains `retryable: bool`.** It is true when a
   later resend of the same turn can succeed and false when a resend
   repeats the failure. The `error` field keeps its content and its role
   as the text to show.
2. **`LlmError::is_retryable` makes the decision for a provider failure.**
   It is true for every error that `is_recoverable` accepts and for
   `LlmError::RateLimited`. A rate limit is not recoverable inside the
   bind loop, which answers it on its own schedule, but it only says to
   wait, so a later resend can pass. Every other error is a verdict on the
   request or the configuration and is not retryable.
3. **`ProviderResponse::Error` becomes `Error { message, retryable }`.**
   The bind loop fills `retryable` from `LlmError::is_retryable` of the
   error it gives up on, at every place it closes a turn with an error:
   an opener error, a terminal error from the stream, the end of the
   reconnects and a spent rate-limit budget. `message` is the text it sent
   before. The ingestion reader copies both into the `StreamError`.
4. **An abnormal stop is never retryable.** The reader emits its
   `StreamError` with `retryable: false`, for the reasons in the decision
   below.
5. **An actor failure is never retryable.** The four failures of the actor
   emit their `StreamError` through one function that sets
   `generation: None` and `retryable: false`.
6. **Every consumer in the repository reads the new shape.** The title
   derivation in `metadata` reads `message` and backs off as before.

## Decision

Decided 2026-10-11.

**The shape is a bool.** The question a consumer asks is whether to resend
the turn, and the answer is yes or no. Rejected alternatives:

- *An enum with two values, such as `Retryable` and `Final`.* It says what
  the bool says, with two more names.
- *An enum of causes, such as overloaded, rate limited, connection lost
  and rejected.* Each consumer would then decide again which cause is
  worth a resend, and the event would repeat the vocabulary of `LlmError`.
- *`LlmError` inside `ProviderResponse::Error`, decided in the reader.*
  The bind loop builds the text for the end of its reconnects from more
  than the error ("Connection interrupted after 5 attempts: ..."), and the
  title derivation in `metadata` reads only the text. The bind loop is the
  one place that knows which error it gave up on.
- *The server's wait hint on the event.* A consumer that resends uses its
  own backoff, and this slice answers only whether a resend can pass. A
  hint can be added later as its own field without changing this one.
- *`retryable` equal to `is_recoverable`.* That would call a spent rate
  limit final, and a rate limit is the clearest case of a failure that a
  later resend fixes.

**No abnormal stop is retryable.**

- *`max_tokens`.* The bound path sets no ceiling of its own:
  `TurnRequest::into_completion` sets `max_tokens` to `None`. The chat
  completions, Responses and Kimi wires then leave the field out, so the
  provider applies its own maximum, and the Anthropic wire, which must send
  a ceiling, sends 32768 on every request. The provider reports the stop
  (`length`, `max_output_tokens` or `max_tokens`) when the turn reaches
  that ceiling or the end of the context window. A resend sends the same
  ceiling, and the reader has committed the text streamed before the stop,
  so the resent history is longer than the one that hit the limit. The
  resend meets the same limit or meets it sooner. What helps is a change
  to the conversation, such as a compaction or a shorter request, and a
  resend does not make one.
- *`content_filter`.* The filter of the provider judged the content of the
  request or the answer, and a resend asks for the same content.
- *`refusal`.* The provider or the model declined the request, and a
  resend sends the same request.

**No actor failure is retryable.**

- *A failed draft promotion or insert of user blocks.* It happens before
  a turn is dispatched, and the turn a consumer would resend does not
  contain the input that failed to append. A failed insert of user blocks
  loses the only copy of that input, which the event contained. The
  consumer recovers by appending the input again, which is a new intent.
- *A failed provider bind.* The conversation, the provider instance, the
  provider module or its configuration is missing, or a read from the
  store failed. A resend binds against the same state.
- *A ledger that does not open with its head kind.* A resend meets the
  same ledger and dispatches nothing.

Rejected: classifying the store error at the actor sites, so that a
contended database counts as retryable. The append sites concern input,
not a turn, and the store already waits on a busy writer before it reports
contention.

## What does not change

- The text of `error` at every site, and both of its vocabularies.
- The bind loop: its reconnects, its rate-limit budget, its backoff, its
  status lines, the payload-omitting fallback and the single `Done`.
- `LlmError::is_recoverable` and every vendor wire.
- What the reader does on an error: it discards the streaming tails,
  resets the turn and ends it with one `StreamError`.
- The actor: every `StreamError` closes the dispatch and latches the
  conversation, whatever `retryable` says. A resend is still the
  consumer's choice, made by releasing the latch.
- The generation stamp on every `StreamError`.
- The title derivation in `metadata`, which backs off and retries on every
  error as before.

## Acceptance criteria

1. `LlmError::is_retryable` is true for `Stream`, `StreamIdle`,
   `Unavailable`, `Api` with status 500, 503 or 529, an `Http` error from
   the transport, and `RateLimited`. It is false for `Api` with status
   400, 401 or 404, `ProviderFailure`, `Json`, `MissingKey`, `Config` and
   `NoMessage`.
2. A turn whose stream keeps dropping closes with one
   `ProviderResponse::Error` that names the end of the reconnects and is
   retryable.
3. A turn whose stream reports a rate limit after content, and a turn
   whose stream reports a rate limit until the budget is spent, each close
   with one retryable `ProviderResponse::Error`.
4. A turn whose stream fails with a 4xx closes with one
   `ProviderResponse::Error` that names the error and is not retryable.
5. The reader turns `ProviderResponse::Error` into exactly one
   `StreamError` with the same text and the same `retryable`, for both
   values.
6. A turn that ends with `max_tokens`, `content_filter` or `refusal`
   closes with exactly one `StreamError` whose `error` is the key and
   whose `retryable` is false.
7. The refusal to dispatch over a ledger that does not open with its head
   kind, and a failed draft promotion, each emit a `StreamError` with
   `generation: None` and `retryable: false`.
8. `cargo test --workspace --all-features`, `cargo clippy --workspace
   --all-features --all-targets -- -D warnings` and `cargo fmt --all
   --check` are clean.
