# Slice 25: the Anthropic wire caches the prompt and reports the cache

Date: 2026-10-11.

The Anthropic Messages API can cache a prompt prefix and bill a later request
that repeats it at a fraction of the input price. A request opts in through
`cache_control`. The facts below come from the prompt caching page of the
Anthropic API documentation
(<https://platform.claude.com/docs/en/build-with-claude/prompt-caching>),
read on 2026-10-11.

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

1. **The caller asks for caching per request.** `ProviderRequest::Stream`,
   the bind loop's `TurnRequest` and `CompletionRequest` gain the flag
   `cache_prompt`. A caller sets it when a later request extends the prompt
   of this one. The session actor sets it on every conversation turn. The
   title derivation leaves it unset: its prompt ends in an instruction that
   no later request repeats, and it runs on another model than the
   conversation.
2. **The Anthropic wire turns on automatic caching for such a request.**
   When `cache_prompt` is set, the request body contains the top-level field
   `"cache_control": {"type": "ephemeral"}`. The API then places one
   breakpoint on the last cacheable block of the request and moves it
   forward as the conversation grows. The next round reads everything the
   previous round wrote and writes only what was added since. A consumer
   configures nothing. The entry has the default lifetime of five minutes.
   Without the flag the body is the same as before this change.
3. **The neutral usage gains two cache counts.** `Usage` and `StreamUsage`
   gain `cache_read_tokens` and `cache_write_tokens`, both optional. Each one
   is `None` when the provider did not report it, never a zero it did not
   report, as `reasoning_tokens` already is. The ingestion reader copies both
   onto the stream-done event beside the other counts.
4. **The neutral input count stays the whole prompt.** The Anthropic wire
   reads the three input counts of the API into a typed structure and
   reports `input_tokens` as their sum, and the two cache counts as the part
   of that sum read from the cache and the part written to it. Each count is
   taken from `message_delta` when it is present there and from
   `message_start` otherwise. A count that is not a whole number in the range
   of the count type, and a sum outside that range, fail the turn with a
   parse error. The sum needs every part, so a part read as absent would make
   the input count too small without any sign of it.

### Decision and rejected alternatives (2026-10-11)

Automatic caching was chosen because it fits how the library builds a
request. The ledger is append-only, so every request repeats the previous
request and adds to its end. That is exactly the case the single moving
breakpoint covers, and the documentation recommends automatic caching for
multi-turn conversations.

- **Explicit breakpoints on the last tool, the system prompt and the last
  message** were rejected. They cache the same prefix as the one automatic
  breakpoint, but the wire would have to track which blocks have a marker
  and limit their number to four per request. They would also change the
  shape of the rendered content: the system prompt would become an array of
  blocks, and a marker would be written onto a message block.
- **An explicit breakpoint on the system prompt in addition to automatic
  caching** was rejected. It would let a new conversation read the tools and
  the system prompt that an earlier conversation cached. That pays off only
  when conversations with an identical system prompt start within the
  lifetime of the cache, which no consumer has asked for.
- **The one-hour lifetime** was rejected. It doubles the price of every
  write, and the rounds of a turn start well within five minutes of each
  other. Each cache read starts the five minutes again.
- **Caching every request** was rejected. A request whose prompt ends in
  content that no later request repeats, such as the title derivation, pays
  the write price of 1.25 times the input price for an entry that is never
  read.
- **A provider setting that turns caching off** was rejected. The library
  has no use for an uncached conversation turn, and a prefix shorter than
  the minimum that the model caches is sent uncached by the API without an
  error.
- **A second request without `cache_control` after a refusal** was rejected.
  The documentation lists the field as accepted on the Claude API, Claude
  Platform on AWS, Amazon Bedrock, Google Vertex AI and Microsoft Foundry.
  The one integration it lists as refusing the field, the legacy Amazon
  Bedrock integration, signs each request with AWS credentials and uses
  another path. This provider sends an API key to `/v1/messages` and cannot
  address that integration.

### Limits of the API that the change relies on

Each limit comes from the documentation page named at the top, read on
2026-10-11.

- **Four breakpoints per request.** The automatic breakpoint uses one of
  them, and the wire sets no other. Cost: none for this change.
- **A lookback of 20 positions.** A breakpoint looks back at most 20
  positions for an earlier cache entry. A run of consecutive tool calls
  counts as one position, and so does a run of consecutive tool results. One
  round of this library appends an assistant message and the user message
  with its results. Cost: a round that appends more than 20 positions misses
  the previous entry, and the request writes the whole prompt again at the
  write price.
- **A cache per model.** A request for one model never reads a cache written
  for another model. Cost: a conversation that switches its model writes its
  prompt again once.
- **A minimum cacheable prefix**, between 512 and 4096 tokens depending on
  the model. Cost: a shorter prompt is sent and billed uncached, without an
  error.
- **A lifetime of five minutes**, started again by every read. Cost: a round
  that starts more than five minutes after the previous one writes the
  prompt again.

## Effect on the other providers

The two new counts are part of the neutral usage type, so every vendor
constructs them. The chat completions dialect, and Kimi and the other vendors
built on it, read `prompt_tokens_details.cached_tokens` as the read count.
The Responses dialect reads `input_tokens_details.cached_tokens`. Neither
dialect reports a write count, so the write count is `None` for both. Their
`input_tokens` already counts the whole prompt, cached tokens included, so
the neutral meaning of the input count is the same for every vendor.

The other vendors build their request without the `cache_prompt` flag.

## What does not change

- The rendered conversation. Tools, the system prompt and every message are
  sent with exactly the content and shape they had. A conversation turn
  gains one top-level field and nothing else.
- The output count, the reasoning count and the stop reasons.
- How the wire joins every system message into the one `system` parameter.
  A system message added later in a conversation, such as a date marker,
  still changes the system prompt. The request that first contains it
  therefore misses the cache from the system prompt onward.
- The requests of the other vendors.

## Acceptance criteria

1. An Anthropic request with `cache_prompt` set sends
   `"cache_control": {"type": "ephemeral"}` at the top level of the request
   body, with or without tools, reasoning and a response schema.
2. An Anthropic request without `cache_prompt` sends no `cache_control`.
3. A request that replays a tool round has no `cache_control` on any tool,
   on the system prompt or on any message block, and its tools, system
   prompt and messages are the same as without caching.
4. A `message_start` with 100 uncached, 2000 written and 30000 read input
   tokens, followed by a `message_delta` that restates none of them, ends
   the turn with `input_tokens` 32100, `cache_write_tokens` 2000 and
   `cache_read_tokens` 30000.
5. A `message_delta` that restates the input counts takes precedence over
   the counts from `message_start`. A `message_delta` that restates only
   `input_tokens` takes that count from the delta and the two cache counts
   from `message_start`.
6. A stream that reports no cache counts ends the turn with both cache
   counts `None` and `input_tokens` equal to the reported input count.
7. An input count that is negative or above the range of the count type,
   and input counts whose sum is above that range, fail the turn with a
   parse error.
8. The stream-done event contains the two cache counts that the provider
   reported for the turn.
9. The chat completions and Responses dialects report `cached_tokens` as
   `cache_read_tokens`, and `cache_write_tokens` as `None`.
10. `cargo test --workspace --all-features`, `cargo clippy --workspace
    --all-features --all-targets -- -D warnings` and `cargo fmt --all
    --check` are clean.
