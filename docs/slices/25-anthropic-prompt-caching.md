# Slice 25: the Anthropic wire caches the prompt and reports the cache

Date: 2026-10-11.

The Anthropic Messages API can cache a prompt prefix and bill a later request
that repeats it at a fraction of the input price. A request opts in through
`cache_control`. The facts below come from the Anthropic prompt caching
documentation, read on 2026-10-11.

## What is wrong today

1. **No request asks for caching.** The Anthropic wire sends no
   `cache_control`, neither at the top level nor on a content block. An agent
   turn that runs several tool rounds sends the whole conversation again on
   every round: the tools, the system prompt and every earlier message. The
   API bills all of it at the full input price each time, so the cost of a
   turn grows with the square of its round count.
2. **The usage ignores the cache counts.** The API reports three input
   counts: `input_tokens`, which counts only the tokens after the last cache
   breakpoint, `cache_creation_input_tokens` and `cache_read_input_tokens`.
   The wire reads only `input_tokens`. Once caching is on, the reported input
   count would shrink to the uncached tail of the prompt, and nothing would
   show how much was read from or written to the cache.

## The change

1. **Every Anthropic request turns on automatic caching.** The request body
   contains the top-level field `"cache_control": {"type": "ephemeral"}`.
   The API then places one breakpoint on the last cacheable block of the
   request and moves it forward as the conversation grows. The next round
   reads everything the previous round wrote and writes only what was added
   since. A consumer configures nothing. The field is sent on every request,
   for every model, with the default lifetime of five minutes.
2. **The neutral usage gains two cache counts.** `Usage` and `StreamUsage`
   gain `cache_read_tokens` and `cache_write_tokens`, both optional. Each one
   is `None` when the provider did not report it, never a zero it did not
   state, as `reasoning_tokens` already is. The ingestion reader carries both
   onto the stream-done event beside the other counts.
3. **The neutral input count stays the whole prompt.** The Anthropic wire
   reports `input_tokens` as the sum of the three input counts of the API,
   and the two cache counts as the part of that sum read from the cache and
   the part written to it. Each count is taken from `message_delta` when it
   is present there and from `message_start` otherwise. A sum that does not
   fit the count type reads as zero, which is how the wire already reads an
   oversized single count.

### Decision and rejected alternatives (2026-10-11)

Automatic caching was chosen because it fits how the library builds a
request. The ledger is append-only, so every request repeats the previous
request and adds to its end. That is exactly the case the single moving
breakpoint covers, and the documentation recommends automatic caching for
multi-turn conversations.

- **Explicit breakpoints on the last tool, the system prompt and the last
  message** were rejected. They cache the same prefix as the one automatic
  breakpoint, but the wire would have to track which blocks carry a marker
  and keep the count within the limit of four breakpoints per request. They
  would also change the shape of the rendered content: the system prompt
  would become an array of blocks, and a marker would be written onto a
  message block.
- **An explicit breakpoint on the system prompt in addition to automatic
  caching** was rejected. It would let a new conversation read the tools and
  the system prompt that an earlier conversation cached. That pays off only
  when conversations with an identical system prompt start within the
  lifetime of the cache, which no consumer has asked for.
- **The one-hour lifetime** was rejected. It doubles the price of every
  write, and the rounds of a turn start well within five minutes of each
  other. Each cache read starts the five minutes again.
- **A setting that turns caching off** was rejected. The library has no use
  for an uncached request, and a prefix shorter than the minimum that the
  model caches is sent uncached by the API without an error.

### Limits of the API that the change relies on

- The automatic breakpoint uses one of the four breakpoints a request may
  carry. The wire sets no other breakpoint.
- A breakpoint looks back at most 20 positions for an earlier cache entry. A
  run of consecutive tool calls counts as one position, and so does a run of
  consecutive tool results. One round of this library appends an assistant
  message and the user message with its results, which is far below 20
  positions.
- A request for one model never reads a cache written for another model.

## Effect on the other providers

The two new counts are part of the neutral usage type, so every vendor
constructs them. The chat completions dialect and the vendors built on it,
the Responses dialect and Kimi report `None` for both. Their `input_tokens`
already counts the whole prompt, cached tokens included, so the neutral
meaning of the input count is the same for every vendor.

## What does not change

- The rendered conversation. Tools, the system prompt and every message are
  sent with exactly the content and shape they had. The request gains one
  top-level field and nothing else.
- The output count, the reasoning count and the stop reasons.
- How the wire joins every system message into the one `system` parameter.
  A system message added later in a conversation, such as a date marker,
  still changes the system prompt and therefore misses the cache from the
  system prompt onward on the request that first carries it.
- The requests of the other vendors.

## Acceptance criteria

1. A bound Anthropic turn sends `"cache_control": {"type": "ephemeral"}` at
   the top level of the request body, with or without tools, reasoning and a
   response schema.
2. A request that replays a tool round carries no `cache_control` on any
   tool, on the system prompt or on any message block, and its tools, system
   prompt and messages are the same as without caching.
3. A `message_start` with 100 uncached, 2000 written and 30000 read input
   tokens, followed by a `message_delta` that restates none of them, ends
   the turn with `input_tokens` 32100, `cache_write_tokens` 2000 and
   `cache_read_tokens` 30000.
4. A `message_delta` that restates the input counts takes precedence over
   the counts from `message_start`.
5. A stream that reports no cache counts ends the turn with both cache
   counts `None` and `input_tokens` equal to the reported input count.
6. The stream-done event carries the two cache counts that the provider
   reported for the turn.
7. `cargo test --workspace --all-features`, `cargo clippy --workspace
   --all-features --all-targets -- -D warnings` and `cargo fmt --all
   --check` are clean.
