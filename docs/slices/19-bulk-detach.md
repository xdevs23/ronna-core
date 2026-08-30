# Slice 19 — detaching many blocks in one transaction

Date: 2026-08-30. A consumer compacting a conversation detaches a large set of
blocks from a fork — the motivating case is a thousand-row tool flood — and the
only door today is `Store::detach_block`, one round trip and one transaction
per block. A thousand detaches serialize a thousand transactions behind
whatever lock the consumer holds, which turns a background sweep into a
seconds-long stall. The per-block door is the right shape for one row and the
wrong shape for a sweep; this slice adds the sweep's own door.

## What this slice builds

`Store::detach_blocks(conversation_id, block_ids)` beside `detach_block`:

- Detaches every named block from the conversation's junction in ONE
  transaction — one round trip, one commit, however many rows.
- Exactly `detach_block`'s semantics per row, stated in both docs as one
  contract: the junction row alone is removed; the block itself, its content,
  and its membership in every other conversation stand untouched.
- An empty list is a no-op that opens no transaction.
- An id with no junction row in this conversation is simply absent from the
  effect, exactly as `detach_block` treats it — the call detaches what is
  there and is not an existence check.
- The two doors share one implementation of the row predicate: the bulk form
  is the loop moved inside the transaction, not a second spelling of the
  delete. If sharing means the single form delegates to the bulk form with a
  one-element list, that is the right shape.

## Acceptance criteria

- AC1: a many-block detach lands atomically — a set of blocks detached through
  the bulk door is gone from the conversation's projection in one step, and
  the blocks remain readable in a sibling conversation that shares them.
  Pinned.
- AC2: the empty list is a no-op and an unknown id detaches nothing while the
  rest of its list still lands. Pinned.
- AC3: the single-row door's behavior is unchanged — its existing pins pass
  untouched.
- AC4: the checks pass: fmt, clippy with warnings denied, the full suite, the
  doc build, exit codes read bare.

## Bounds

- No schema change, no new dependency. One public method, its doc, its tests.
- No caller changes in this repository; the consumer that needs the sweep
  calls it from its own commit.
