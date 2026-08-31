# Slice 20 + consumer unit — compaction

Date: 2026-08-30, the date the design was decided. The work spans two
repositories: the framework mechanism here, and the consumer wiring in the
application built on it.

## What exists, and what this replaces

Inventory of the substrate, all committed:

- The app's `Sessions` (crates/core/src/session.rs): `map_new_channel` with
  the claim WINNER CHECK, `forked_with_current_prompt`, the mapping swap
  under the global stamp lock + erasure fence, the racing-ingest fix (the
  ingest resolves its mapping INSIDE the lock), the claim-lost silence, the
  reset directive on `IngestOutcome::Recorded`, `/wipe`, and the
  exhausted-turn watcher (`spawn_auto_compact`) on the framework's
  `tool_calls_exhausted` status — the runaway signal, already wired.
- The framework's `fork_continuation` (store/conversations.rs): a
  one-transaction fork walking MESSAGE-GROUP BOUNDS (`find_group_bounds`),
  with `copy_junction_up_to` / `copy_junction_before` and
  `confirm_inherited_history`. The group-bounds walk is the precedent for
  cutting the ledger at a place that never splits a MESSAGE GROUP. It is
  role-contiguous only: its existing callers stay safe on tool lifecycles
  by anchoring on user messages, so item 1 adds the outcome-extension rule
  for a cut that lands elsewhere — a model-voiced group can trail in tool
  calls whose results sit in the following roleless group.
- `Store::detach_blocks` (slice 19): the bulk junction operation.
- The app's stream observer sees `CoreEvent::StreamDone` with every
  request's usage; the framework persists no usage.

REPLACED by this work: unit 45's tail-keep mechanism (`compaction.rs`'s
kept-set plan and its spine pins) — the design has no tail-keep; one
compaction, recorded once. `/compact` and the exhausted-turn signal both drive
THE mechanism below. Everything else in the inventory is kept and used.
Unit 45's decision records that describe the tail-keep (0163 above all) gain
a dated supersession note; the racing/claim decisions (0161-0166's race
decisions) stand untouched.

## The mechanism, mapped to the design

Framework primitives (this repository — no consumer vocabulary, the
instructions text and all policy arrive as parameters):

1. **Half the ledger, forked into a temporary conversation** — a fork of the
   FIRST half: the cut point is DETERMINISTIC — find the block at half the
   ledger by block count, resolve the message group that contains it (the
   `find_group_bounds` discipline), and the first half ends at that group's
   LAST block, inclusive — then the cut EXTENDS forward, minimally and
   deterministically, while any tool call inside the first half has its
   outcome beyond it: each extension moves the cut to the LAST block of the
   GROUP containing the answering outcomes (group granularity, never
   mid-group — roleless groups mix result, error and marker blocks),
   repeating until no call before the cut is answered after it, so the second
   half never opens mid-group or on an orphaned tool outcome and no lifecycle
   is split. Half the ledger is measured by BLOCK COUNT — a design-review
   note, the design being silent on the measure, like the tiebreak below. A
   group straddling the half point therefore lands whole in the summarized
   half, and the second half always begins at a clean group start. No group
   or tool lifecycle is ever split. The temporary conversation carries the
   first half's junction rows. The whole-into-the-first-half tiebreak is this
   spec's refinement, a design-review note: the opposite side is equally
   consistent with taking half the ledger; the first-half side is chosen so a
   straddling exchange is summarized whole instead of splitting its opening
   from its summary.
2. **A harness message carrying the compaction instructions** — the temporary
   conversation gets the caller-provided instructions appended as a harness
   message; appending it leaves the temporary conversation owing a model
   turn, so the actor drives it like any other.
3. **No tools provided** — the temporary conversation records an EMPTY tool
   palette, and the turn the harness message summons is offered no tool
   definitions at all.
4. **The response captured** — the temporary conversation's completed
   answer text IS the summary. The caller observes the completion through
   the bus it already watches and reads the answer from the ledger.
5. **A new thread whose first content references the old conversation in a
   database column** — a new conversation whose FIRST content is a new
   ancestor-reference block kind: its row carries the source conversation's
   id in its own COLUMN (a numbered migration step; the shipped CREATE is
   never edited), and its projection names nothing personal.
6. **Then the compaction message** — TWO SEQUENTIAL APPENDS, exactly as the
   design orders them: first the ancestor-reference block (item 5), THEN the
   compaction message carrying the captured summary as its own append. No
   fusing into one block — the design appends a block that references the old
   conversation and then appends the compaction message, and two appends is
   what the code does.
7. **Then the second half of the original ledger** — the second half's
   junction rows are copied onto the new conversation
   (`confirm_inherited_history` so the outbound edge is born delivered, the
   unit-45 seam).
8. **Then it runs like any other conversation** — the new conversation is
   mapped in the channel's place through the existing claim-with-winner-check
   swap; nothing else about serving it is special. The temporary conversation
   is retired junction-only after capture (the first half's blocks all live
   on in the source; the temporary conversation's own two appends — the
   instructions message and the captured response — become orphans for the
   collector once the summary text is carried forward; the source itself
   stays whole, unmapped, exactly as unit 45's sources do).

Consumer wiring (the app repository):

9. **The trigger, both arms**: the first arm is only 50k of context left —
   the context window size comes from the model configuration, the used
   amount from the last turn's usage (BUILD WORK in the app: today's stream
   observer drops the usage field `CoreEvent::StreamDone` already carries;
   this unit makes it hold the latest usage per conversation, in memory; the
   framework persists none; a turn whose provider reported no usage leaves
   the last known number standing, and with none ever known BOTH arms stay
   silent — the trigger never fires blind; that absent-data behavior is a
   design-review note, the other two doors being unaffected). The second arm
   is an expired KV cache with more than half the context window used: cache
   expiry is time-since-last-dispatch beyond a named, configurable TTL
   constant (the provider's cache lifetime; the constant's doc says it is an
   estimate of an external fact). The TTL proxy is the only locally
   measurable reading of an expired KV cache and is a design-review note like
   the readings in items 1, 10 and 13.
10. **The timing**: the design puts the compaction in a quiet window and
    keeps it off an expired KV cache. Read as: quiet is the preference, a
    warm cache the hard constraint, and the rule is ONE and the same
    whichever arm armed the trigger, tracking the CURRENT cache state. Once a
    threshold arm holds, compact at the next quiet moment (no new inbound for
    a named number of minutes); whenever the cache is warm (the last dispatch
    lies within the TTL) and quiet would arrive only past the TTL edge, go
    just BEFORE the edge instead (a named safety margin ahead of it, so the
    dispatch itself still lands on the warm cache); only while the cache is
    already expired AND nothing has re-warmed it does the next quiet moment
    suffice — there is no warm window left to protect. Every dispatch
    re-warms the cache and restarts the same rule from the new edge, so an
    armed trigger under continuing traffic never knowingly dispatches
    full-price into an expired cache. This reading is a design-review note.
11. **Three doors, one mechanism**: `/compact` runs THE mechanism now,
    thresholds ignored; `/wipe` creates a brand new session; the
    exhausted-turn signal (the watcher unit 45 wired) runs the mechanism
    unattended. The design names five consecutive rate-limit errors the MODEL
    hits, and that names the wired signal exactly: five consecutive TOOL-CALL
    rate-limit refusals are the one kind of rate-limit error the model itself
    hits and answers; provider-level 429s never reach the model (the
    transport retries them), so no wider signal exists for the model to hit.
12. **The compaction-instructions prompt** — the app's copy, written from the
    design's ask to mention the important things and the conversational
    topics: instructions telling the model to compact the first half, naming
    the important things and the conversational topics, byte-pinned once
    written.
13. **Erasure**: the design scrubs an erased principal's words off the
    compaction, and the mechanism is CLONE-STRIP-DELETE, copy-on-write —
    blocks are cloned only if they changed; the junction table is the sharing
    the store already has, `detach_blocks` the delete primitive — applied to
    EACH conversation in the lineage whose history must lose blocks. The
    digest and the words that fed it live on DIFFERENT conversations (the
    first half on the old source, the compaction message on the serving
    thread), so the flow names both:

    - The SOURCE conversation is cloned minus the erased principal's
      blocks; every other junction row is shared, never copied. The clone
      IS the post-erasure source history.
    - The regenerated compaction message is captured from that source
      clone the way the original was (items 2-4): a temporary fork, the
      same instructions, an empty palette, the captured answer taken as
      the new digest. The SPAN is pinned, not re-derived: the regeneration
      covers exactly the source clone's blocks that the serving thread did
      NOT inherit — the original first half as it stands after the strip.
      The boundary needs no stored position; it is the complement of the
      serving thread's inherited junction rows. No exchange silently drops
      from the serving view and none is double-reported beside the
      verbatim second half.
    - The SERVING conversation is cloned minus any of the erased
      principal's blocks in its own history, with its two opening appends
      changed in place of the old pair: the ancestor-reference block now
      naming the source clone, THEN the regenerated compaction message —
      the item-6 order. Everything else, the second half included, is
      shared.
    - The serving clone takes the channel through the existing
      claim-with-winner-check swap (item 8's mechanism); only after the
      swap is verified are the old source and the old serving thread
      deleted junction-only.
    - Ordering is capture-first: nothing is swapped and nothing is deleted
      until the regeneration capture completes; a failed or empty capture
      leaves every existing conversation standing and the scrub retries.
      The stored personal data itself is erased immediately through the
      erasure machinery the store already has — the scrub completes the
      erasure of the digest, it never delays the data erasure.

    Design-review notes: scrubbing a model-written digest means regeneration,
    because prose cannot be mechanically stripped of one voice; and the
    design names the conversation in the singular, which is read as the one
    mechanism applied per affected conversation, the lineage being two. No erased words survive
    in a digest.

## Acceptance criteria

- AC1: both trigger arms fire exactly per the design — pinned over injected
  usage numbers, window sizes and clocks at the policy function.
- AC2: the mechanism end to end through the scripted harness: first-half
  temp fork with instructions and an empty palette; the summary captured;
  the new conversation carrying ancestor-reference column + compaction
  message + second half; the mapping swapped with the winner check; the next
  member message answered from the compacted conversation as usual.
- AC3: `/compact` and the exhausted-turn signal both drive the one mechanism —
  pinned; the tail-keep plan is gone and a grep finds no second compaction
  shape.
- AC4: the quiet-window/TTL timing decides per the stated reading — pinned
  at the policy function with injected clocks.
- AC5: the erasure decision — pinned end to end on a compacted lineage:
  erase a principal whose words fed the digest; the source clone omits the
  erased blocks (unchanged rows shared — pinned by block identity, never
  copies), the regenerated digest carries no erased words, the regeneration
  SPAN equals the original first half minus the erased blocks (pinned: the
  temporary fork's content is the complement of the serving thread's
  inherited rows — nothing dropped, nothing double-reported), the serving
  clone carries the new ancestor reference then the new digest with its
  second half shared, the channel swap lands BEFORE the two old
  conversations are deleted junction-only, and a capture that fails leaves
  the originals standing.
- AC6: unit 45's race pins keep passing (racing ingest lands in the
  survivor; a lost claim answers silence; junction-only cleanup).
- AC7: both repositories' checks pass — fmt, clippy -D warnings, the full
  suite, the doc build, exit codes read bare, run in EACH repository.
- AC-DESIGN: a dedicated adversarial review judges the finished code against
  the design above, statement by statement; every divergence is a must-fix,
  and none may be silently accepted.

## Bounds

- No new dependency in either repository. The framework knows no consumer
  vocabulary; every policy number and text arrives as a parameter or lives
  in the app.
- The ancestor-reference column is the design's own database column,
  committed as a numbered migration step with its select/fold/parse, per the
  numbered-migration discipline (docs/slices/13-dated-consumer-appends.md).
- Decision records: the app numbers from its next free (0170+); the
  framework's slice doc is this file. Unit 45's superseded records gain
  dated notes, never silent edits.

## As built, 2026-08-31

The mechanism is in both repositories. What follows records where it lives,
what the tree forced, and the choices the design does not decide — the last
of these are design-review notes, exactly as the readings above are.

### The framework

- `store/compaction.rs` — the whole storage half. `Store::compaction_cut`
  answers where a ledger splits; `Store::fork_temporary` builds the
  temporary conversation and returns the instructions block whose turn
  produces the answer; `Store::open_compacted_thread` writes the prompt, the
  ancestor reference, the compaction message and the second half's junction
  rows, in one transaction.
- `agency/ancestor_reference.rs` — the design's database column, as a block
  kind of its own. Migration step v5 creates `block_ancestor_reference` with
  the select, the fold and the parse the numbered-migration discipline asks
  for. The column carries NO foreign key, deliberately: an erasure replaces
  an ancestor with a scrubbed clone and deletes the original, and the record
  of where a thread came from has to survive that.
- `agency/harness_message.rs` — the harness's own message to the model, a
  block kind of its own, and the two facts that make item 2 and item 3 work
  at all: it awaits the model, and it offers no tools.
- `agency/text.rs` — unchanged in what it asks for: a user's prose asks the
  model for a turn and every other voice's asks for nothing.
- `actor.rs` — the dispatch reads what the turn is offered off the frontier
  block's own kind, so the site still names no kind.
- `agency/ratchet.rs` — the resume anchor takes the cursor's own block
  first.

### What the tree forced, and how it was answered

Three premises in the prose above are false against the tree as it stood.
None changes what the design asks for; each is recorded here with what was
built to make the design's own sentence true.

1. **"appending it leaves the temporary conversation owing a model turn"** —
   it did not. A system-voiced `text` block awaited nothing, so the actor
   would never have driven the temporary conversation and no answer could have
   been captured. Making system-voiced prose await the model was the first
   answer and the wrong one: the compaction message a compacted thread opens
   with is system-voiced prose too, sitting in a live serving thread, and it
   would then have been one frontier away from summoning an unasked turn.
   The ask is a fact about the harness ASKING, not about a voice any append
   can carry, so it lives on a kind of its own —
   `agency/harness_message.rs` — which the library writes from exactly one
   place (`Store::fork_temporary`) and no consumer can write at all. Text
   answers exactly what it always answered: only a user's prose asks for a
   turn.

2. **"the model of that turn is offered nothing"** — an empty tool palette
   is the consumer's ADMISSION record, and the consumer's own documentation
   says so: it gates admission, not exposure, and the model is still offered
   every registered tool. The design says no tools are provided, which is
   about the request. So the harness-message kind answers that too: it
   offers no tools, the dispatch reads that off the frontier block, and the
   request carries an empty definition list. The empty palette is recorded
   as well, so a provider that ignored the empty list would still have every
   call declined.

3. **"the second half's junction rows are copied onto the new conversation"**
   — sound, and it breaks a monotonicity the ratchet's resume rested on: the
   thread's own three appends carry higher ids than the rows it inherits, so
   ids DESCEND at that seam. Anchoring the cursor by id then landed on the
   fresh front block and re-derived the whole ledger on every drive. The
   resume now takes the cursor's own block first and keeps the id scan as
   the fallback for a cursor whose block was detached. The outbound edge's
   inherited boundary needed nothing: it reads the newest SHARED block,
   which is exact whatever the order.

### The choices the design does not decide

Design-review notes, beside the readings the prose above already flags.

- **The near-side fallback.** A group running from the half point to the
  ledger's tail — a long unanswered run of one voice — would take everything
  into the summarized half and leave nothing to carry forward, so the
  conversation that most needs compacting could not be compacted. When the
  far side leaves no second half, the cut falls back to the NEAR side of the
  same group: the group rides across verbatim instead of being summarized.
  It is the other reading of taking half the ledger, used only where the
  preferred one answers nothing.
- **The compaction message's voice.** System. The harness is stating what
  the earlier history held; the model is not recalling it. Every wire this
  deployment reaches folds system messages into its system parameter, so the
  digest reads as context ahead of the verbatim half.
- **The new thread's system prompt.** The thread opens with the consumer's
  current prompt, ahead of the ancestor reference, because the prompt is
  configuration rather than content and running the thread like any other is
  not possible without one. The ancestor reference is still the thread's
  first CONTENT block. The framework holds no prompt: it arrives as a
  parameter, exactly as it does at the existing new-thread fork.
- **The new thread has no `parent_id`.** It is not a fork of anything — it
  opens with a summary of a history it does not hold. Where it came from is
  the block's own column, which is the fact the design asked for and the one
  that survives the ancestor's deletion.
