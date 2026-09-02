//! The compaction primitives from outside the crate, the way a consumer
//! reaches them: the cut, the temporary conversation the first half is
//! summarized in, and the thread the summary opens.
//!
//! Two things are provable only from here. That this file compiles is the
//! proof the whole mechanism is reachable from a consumer — the cut, the
//! fork, the thread and the ancestor-reference block's own column. And the
//! turn a compaction runs is observed AT THE PROVIDER, where "dont provide
//! any tools" either holds or does not: the request the harness message
//! summons carries no tool definitions, whatever the registry holds.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use agent_ledger::agency::{
    Agency, AncestorReference, Awaiting, FromBlock, HarnessMessage, LeafKind, SystemPrompt, Text,
    ToolChoice,
};
use agent_ledger::providers::{
    BoxFuture, LlmError, ModelInfo, ProviderModule, ProviderRequest, ProviderResponse, ProviderRx,
    ProviderTx, StreamEvent, ToolDefinition,
};
use agent_ledger::store::{CompactedThread, ConsumerRecord, ModelOverride, TemporaryFork};
use agent_ledger::types::StopReason;
use agent_ledger::{
    BlockKind, CoreEvent, EventBus, ProviderRegistry, Role, RuntimeContext, Store, StoreError,
    ToolContext, ToolHandler, ToolOutcome, ToolRegistry, spawn_reactor,
};
use serde_json::Value;

/// What the consumer's harness asks for. The library ships no prompts, so
/// these words arrive from here, exactly as an embedder's do.
const INSTRUCTIONS: &str = "Summarize the conversation above. Write prose and nothing else.";

/// What the scripted model answers the harness with.
const SUMMARY: &str = "They talked about the release, then about the schedule.";

/// The prompt a compacted thread is opened with, told apart from whatever its
/// source carries.
const THREAD_PROMPT: &str = "You are the consumer's assistant, opening a thread.";

/// A tool the consumer registered — present in the registry, so a turn that
/// is offered nothing is offered nothing DESPITE it.
struct Lookup;

impl ToolHandler<CoreEvent> for Lookup {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "lookup".into(),
            description: "looks something up".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async { ToolOutcome::Done("looked up".into()) })
    }
}

/// A provider that answers every turn with the summary and counts how many
/// tool definitions each request carried.
struct Scripted {
    offered: Arc<std::sync::Mutex<Vec<usize>>>,
    turns: Arc<AtomicUsize>,
}

impl ProviderModule for Scripted {
    fn type_id(&self) -> &'static str {
        "scripted"
    }

    fn display_name(&self) -> &'static str {
        "Scripted"
    }

    fn description(&self) -> &'static str {
        "answers every turn with the summary"
    }

    fn get_config(&self, _provider_id: String) -> BoxFuture<'_, Result<Option<Value>, StoreError>> {
        // A bind needs a configuration to exist; this provider reads
        // nothing from it.
        Box::pin(async { Ok(Some(serde_json::json!({}))) })
    }

    fn save_config(
        &self,
        _provider_id: String,
        _config: Value,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }

    fn delete_config(&self, _provider_id: String) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }

    fn summary(&self, _provider_id: String) -> BoxFuture<'_, Result<Option<String>, StoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn list_models(&self, _config: Value) -> BoxFuture<'_, Result<Vec<ModelInfo>, LlmError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn bind(
        &self,
        _conversation_id: i64,
        _provider_id: String,
        _config: Value,
    ) -> (ProviderTx, ProviderRx) {
        let (request_tx, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let (response_tx, responses) = tokio::sync::mpsc::unbounded_channel();
        let offered = Arc::clone(&self.offered);
        let turns = Arc::clone(&self.turns);
        tokio::spawn(async move {
            while let Some(request) = requests.recv().await {
                let ProviderRequest::Stream { tools, .. } = request else {
                    continue;
                };
                offered.lock().unwrap().push(tools.len());
                turns.fetch_add(1, Ordering::SeqCst);
                let _ = response_tx.send(ProviderResponse::Event(StreamEvent::Connected));
                let _ = response_tx.send(ProviderResponse::Event(StreamEvent::TextBlockStart));
                let _ = response_tx.send(ProviderResponse::Event(StreamEvent::TextDelta {
                    text: SUMMARY.into(),
                }));
                let _ = response_tx.send(ProviderResponse::Event(StreamEvent::MessageEnd {
                    usage: agent_ledger::providers::Usage::default(),
                    stop_reason: StopReason::EndTurn,
                }));
                let _ = response_tx.send(ProviderResponse::Done);
            }
        });
        (request_tx, responses)
    }
}

/// A runtime a consumer could have built, with one tool registered and the
/// scripted provider bound.
async fn consumer_runtime() -> (
    RuntimeContext<BlockKind, CoreEvent>,
    Arc<std::sync::Mutex<Vec<usize>>>,
) {
    let store = Store::in_memory().unwrap();
    store
        .save_provider_instance(agent_ledger::store::ProviderInstance {
            id: "scripted-1".into(),
            provider_type: "scripted".into(),
            name: "Scripted".into(),
        })
        .await
        .unwrap();
    let offered = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut providers = ProviderRegistry::new();
    providers.register(Box::new(Scripted {
        offered: Arc::clone(&offered),
        turns: Arc::new(AtomicUsize::new(0)),
    }));
    let mut tools = ToolRegistry::new();
    tools.register("lookup", Lookup);
    let ctx = RuntimeContext::new(
        store,
        Arc::new(EventBus::<CoreEvent>::new()),
        Arc::new(providers),
        Arc::new(tools),
    )
    .without_title_derivation();
    spawn_reactor(ctx.clone());
    (ctx, offered)
}

/// A conversation with a system prompt and four exchanges — enough that the
/// ledger has two halves.
async fn conversation(ctx: &RuntimeContext<BlockKind, CoreEvent>) -> i64 {
    let store = ctx.store();
    let id = store
        .create_conversation(
            "scripted-1".into(),
            "model".into(),
            "Model".into(),
            String::new(),
        )
        .await
        .unwrap();
    store
        .insert_system_prompt(id, "You are the consumer's assistant.".into())
        .await
        .unwrap();
    for round in 0..4 {
        store
            .insert_text_block(id, Role::User, format!("question {round}"))
            .await
            .unwrap();
        store
            .insert_text_block(id, Role::Assistant, format!("answer {round}"))
            .await
            .unwrap();
    }
    id
}

/// The block ids of one conversation's ledger, in order.
async fn ledger_ids(ctx: &RuntimeContext<BlockKind, CoreEvent>, conversation_id: i64) -> Vec<i64> {
    ctx.store()
        .list_blocks(conversation_id)
        .await
        .unwrap()
        .iter()
        .map(|block| block.id)
        .collect()
}

/// Run the temporary conversation's one turn and read its answer, the way a
/// consumer does: unlatch, then take the newest assistant prose past the
/// instructions block.
async fn captured_answer(
    ctx: &RuntimeContext<BlockKind, CoreEvent>,
    temporary: agent_ledger::store::TemporaryConversation,
) -> String {
    ctx.bus().emit(CoreEvent::UnlatchRequested {
        conversation_id: temporary.conversation_id,
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let answer = ctx
            .store()
            .list_blocks(temporary.conversation_id)
            .await
            .unwrap()
            .iter()
            .rev()
            .take_while(|block| block.id != temporary.instructions_block_id)
            .filter(|block| {
                block.role == Some(Role::Assistant)
                    && Text::KINDS.contains(&block.block_type.as_str())
            })
            .map(|block| Text::parse(block).content)
            .find(|content| !content.is_empty());
        if let Some(answer) = answer {
            return answer;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out awaiting the harness turn"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// The cut and the temporary conversation the first half is summarized in,
/// from the consumer's side.
///
/// The tool count is the one assertion that cannot be made from the ledger:
/// the harness's turn is offered NOTHING, while the registry holds a tool
/// the whole time. What makes that true is in the ledger, and asserted there
/// too — the empty tool choice this door writes itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_consumer_cuts_a_ledger_and_summarizes_its_first_half() {
    let (ctx, offered) = consumer_runtime().await;
    let store = ctx.store();
    let source = conversation(&ctx).await;
    let source_ids = ledger_ids(&ctx, source).await;

    let cut = store
        .compaction_cut(source)
        .await
        .unwrap()
        .expect("a nine-block ledger splits");
    let at = source_ids
        .iter()
        .position(|id| *id == cut.first_half_ends)
        .expect("the cut names a block of the ledger");
    assert_eq!(
        source_ids[at + 1],
        cut.second_half_begins,
        "the second half opens at the block after the cut"
    );

    // The temporary conversation: the first half, an empty consumer record
    // ahead of the instructions, and the instructions last.
    let temporary = store
        .fork_temporary(
            source,
            cut.first_half_ends,
            TemporaryFork {
                records: Vec::<ConsumerRecord>::new(),
                instructions: INSTRUCTIONS.into(),
            },
        )
        .await
        .unwrap();
    let held = store.list_blocks(temporary.conversation_id).await.unwrap();
    let choice_at = held.len() - 2;
    assert_eq!(
        held.iter().map(|block| block.id).collect::<Vec<i64>>(),
        {
            let mut ids = source_ids[..=at].to_vec();
            ids.push(held[choice_at].id);
            ids.push(temporary.instructions_block_id);
            ids
        },
        "the temporary conversation is the first half, the library's own tool choice, \
         and the harness message"
    );
    // AC13 — the library states this conversation's tools, states them EMPTY,
    // and states them BEFORE the block that summons the turn. No consumer
    // supplied it: the fork above was handed no records at all.
    assert_eq!(
        held[choice_at].block_type,
        ToolChoice::KINDS[0],
        "the fork records the choice itself"
    );
    assert!(
        ToolChoice::parse(&held[choice_at]).names.is_empty(),
        "and the choice it records is the empty one"
    );
    assert_eq!(
        store
            .newest_tool_choice(temporary.conversation_id)
            .await
            .unwrap(),
        Some(Vec::new()),
        "so the turn the instructions summon reads an empty choice"
    );
    let instructions = held.last().unwrap();
    assert_eq!(
        instructions.role,
        Some(Role::System),
        "the harness message speaks in the system voice"
    );
    assert_eq!(
        instructions.block_type,
        HarnessMessage::KINDS[0],
        "the ask is a kind of its own, not prose in a particular voice"
    );
    assert_eq!(
        BlockKind::from_block(instructions).awaiting(),
        Some(Awaiting::Model),
        "appending it is what summons the turn"
    );

    // Nothing has run: a fresh conversation boots latched, which is what
    // lets the caller record everything before the turn fires.
    assert!(
        offered.lock().unwrap().is_empty(),
        "the temporary conversation runs nothing until it is unlatched"
    );
    assert_eq!(captured_answer(&ctx, temporary).await, SUMMARY);
    assert_eq!(
        *offered.lock().unwrap(),
        vec![0],
        "the harness's turn is offered NO tools, with one registered the whole time"
    );

    // Retired junction-only: the first half's blocks all live on in the
    // source, and only this conversation's own are left behind.
    store
        .delete_conversation(temporary.conversation_id)
        .await
        .unwrap();
    assert_eq!(
        ledger_ids(&ctx, source).await,
        source_ids,
        "a compaction does not edit what it came from"
    );
}

/// The thread a captured summary opens, from the consumer's side: the
/// prompt, the ancestor reference, the compaction message, and the second
/// half shared by identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_consumer_opens_the_thread_the_summary_carries() {
    let (ctx, _offered) = consumer_runtime().await;
    let store = ctx.store();
    let source = conversation(&ctx).await;
    let source_ids = ledger_ids(&ctx, source).await;
    let cut = store
        .compaction_cut(source)
        .await
        .unwrap()
        .expect("a nine-block ledger splits");
    let at = source_ids
        .iter()
        .position(|id| *id == cut.first_half_ends)
        .expect("the cut names a block of the ledger");
    let captured = SUMMARY.to_owned();

    let thread = store
        .open_compacted_thread(
            source,
            cut.first_half_ends,
            CompactedThread {
                ancestor_conversation_id: source,
                system_prompt: Some("You are the consumer's assistant.".into()),
                compaction_message: captured.clone(),
                model: ModelOverride::default(),
            },
        )
        .await
        .unwrap();
    let blocks = store.list_blocks(thread).await.unwrap();
    assert_eq!(blocks[0].block_type, "system_prompt");
    assert_eq!(
        blocks[1].block_type,
        AncestorReference::KINDS[0],
        "the thread's first content block names where it came from"
    );
    assert_eq!(
        AncestorReference::parse(&blocks[1]).conversation_id,
        source,
        "the ancestry rides a column of the block's own row"
    );
    assert_eq!(
        blocks[2].block_type,
        Text::KINDS[0],
        "the compaction message is its own append, never fused with the reference"
    );
    assert_eq!(Text::parse(&blocks[2]).content, SUMMARY);
    // The digest STATES; it never asks. This thread serves a channel, and the
    // frontier walks back onto the digest whenever everything behind it is a
    // dead turn's leavings — an ask here would dispatch a turn nobody asked
    // for and speak the model's answer into that channel.
    assert_eq!(
        BlockKind::from_block(&blocks[2]).awaiting(),
        None,
        "the compaction message asks the model for nothing"
    );
    assert_eq!(
        blocks[3..]
            .iter()
            .map(|block| block.id)
            .collect::<Vec<i64>>(),
        source_ids[at + 1..],
        "the second half rides across by identity: shared junction rows, never copies"
    );

    // The source is untouched, and it stays the ancestor's readable record.
    assert_eq!(
        store
            .list_blocks(source)
            .await
            .unwrap()
            .iter()
            .map(|block| block.id)
            .collect::<Vec<i64>>(),
        source_ids,
        "a compaction does not edit what it came from"
    );

    // The ancestry outlives its ancestor: the column is a record, not a
    // link, which is what an erasure that replaces an ancestor depends on.
    store.delete_conversation(source).await.unwrap();
    let blocks = store.list_blocks(thread).await.unwrap();
    assert_eq!(
        AncestorReference::parse(&blocks[1]).conversation_id,
        source,
        "the record survives the conversation it names"
    );
}

/// A source carrying its system prompt PAST the cut still compacts. That
/// shape is what a database written before the prompt became the head of its
/// ledger holds, and every one of them is deployed: the thread opens with its
/// OWN prompt, inherits every other post-cut block in order, and the source's
/// late prompt stays in the source.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_whose_prompt_sits_past_the_cut_still_compacts() {
    let (ctx, _offered) = consumer_runtime().await;
    let store = ctx.store();
    let source = conversation(&ctx).await;

    // The forbidden shape, built the only way it can still be built: the
    // block joins the ledger as system-voiced prose — the very header,
    // junction and text rows a prompt is made of — and is then stamped with
    // the prompt's kind. The rule stands on the junction insert, which is
    // exactly where a database written before it never met one.
    let late = store
        .insert_text_block(source, Role::System, "the redeployed instructions".into())
        .await
        .unwrap();
    store
        .run(move |conn| {
            conn.execute(
                "UPDATE blocks SET block_type = 'system_prompt' WHERE id = ?1",
                [late],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let source_ids = ledger_ids(&ctx, source).await;
    assert_eq!(
        source_ids.last(),
        Some(&late),
        "the source's prompt is its newest block"
    );
    let cut = store
        .compaction_cut(source)
        .await
        .unwrap()
        .expect("the ledger splits");
    let at = source_ids
        .iter()
        .position(|id| *id == cut.first_half_ends)
        .expect("the cut names a block of the ledger");

    let thread = store
        .open_compacted_thread(
            source,
            cut.first_half_ends,
            CompactedThread {
                ancestor_conversation_id: source,
                system_prompt: Some(THREAD_PROMPT.into()),
                compaction_message: SUMMARY.into(),
                model: ModelOverride::default(),
            },
        )
        .await
        .unwrap();

    let blocks = store.list_blocks(thread).await.unwrap();
    assert_eq!(
        blocks
            .iter()
            .filter(|block| block.block_type == "system_prompt")
            .count(),
        1,
        "the thread has one prompt"
    );
    assert_eq!(blocks[0].block_type, "system_prompt");
    assert_eq!(
        SystemPrompt::parse(&blocks[0]).content,
        THREAD_PROMPT,
        "and it is the thread's own"
    );
    assert_eq!(
        blocks[3..]
            .iter()
            .map(|block| block.id)
            .collect::<Vec<i64>>(),
        source_ids[at + 1..]
            .iter()
            .copied()
            .filter(|id| *id != late)
            .collect::<Vec<i64>>(),
        "every other post-cut block rides across, in order"
    );

    assert_eq!(
        ledger_ids(&ctx, source).await,
        source_ids,
        "the source keeps the prompt it was written with"
    );
}

/// AC14 — a compacted thread continues the same session, so the tools that
/// session has come across with it: the new thread's newest choice equals the
/// source's, and it is recorded ahead of the inherited history, one of whose
/// blocks may itself owe a turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compacted_thread_carries_the_sources_newest_tool_choice() {
    let (ctx, _offered) = consumer_runtime().await;
    let store = ctx.store();
    let source = conversation(&ctx).await;
    store
        .append_tool_choice(source, vec!["lookup".into()])
        .await
        .unwrap();
    // A later append supersedes the first: what comes across is the NEWEST
    // record, not the first one the session ever made.
    store
        .append_tool_choice(source, vec!["lookup".into(), "recall".into()])
        .await
        .unwrap();
    let cut = store
        .compaction_cut(source)
        .await
        .unwrap()
        .expect("the ledger splits");

    let thread = store
        .open_compacted_thread(
            source,
            cut.first_half_ends,
            CompactedThread {
                ancestor_conversation_id: source,
                system_prompt: None,
                compaction_message: SUMMARY.into(),
                model: ModelOverride::default(),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        store.newest_tool_choice(thread).await.unwrap(),
        store.newest_tool_choice(source).await.unwrap(),
        "the thread continues the session, and so do its tools"
    );
    let blocks = store.list_blocks(thread).await.unwrap();
    let at = blocks
        .iter()
        .position(|block| block.block_type == ToolChoice::KINDS[0])
        .expect("the thread carries the choice");
    assert_eq!(
        ToolChoice::parse(&blocks[at]).names,
        vec!["lookup".to_owned(), "recall".to_owned()]
    );
    let inherited = blocks
        .iter()
        .position(|block| block.id == cut.second_half_begins)
        .expect("the second half rides across");
    assert!(
        at < inherited,
        "the record is in place before the history that may owe a turn"
    );
}

/// AC14, the overlap it leaves open — a source whose newest record lies PAST
/// the cut hands that record across twice: once written by the opening, once
/// inherited with the second half, which carries the source's rows verbatim.
///
/// Both name the same tools, and a later record superseding an earlier one is
/// the kind's own rule, so the copy IS the same decision stated twice and both
/// readers of the thread answer exactly what the source said. Suppressing the
/// second one costs more than it saves: the junction copy is general and
/// carries every kind alike, and teaching it about this one would put the
/// kind's name inside a mechanism that must not know it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_past_the_cut_rides_across_twice_and_answers_the_same() {
    let (ctx, _offered) = consumer_runtime().await;
    let store = ctx.store();
    let source = conversation(&ctx).await;
    let cut = store
        .compaction_cut(source)
        .await
        .unwrap()
        .expect("the ledger splits");
    // Recorded after the cut was taken, so the record sits in the half that
    // rides across verbatim.
    store
        .append_tool_choice(source, vec!["lookup".into()])
        .await
        .unwrap();

    let thread = store
        .open_compacted_thread(
            source,
            cut.first_half_ends,
            CompactedThread {
                ancestor_conversation_id: source,
                system_prompt: None,
                compaction_message: SUMMARY.into(),
                model: ModelOverride::default(),
            },
        )
        .await
        .unwrap();

    let blocks = store.list_blocks(thread).await.unwrap();
    assert_eq!(
        blocks
            .iter()
            .filter(|block| block.block_type == ToolChoice::KINDS[0])
            .count(),
        2,
        "the written record and the inherited one both land"
    );
    assert_eq!(
        store.newest_tool_choice(thread).await.unwrap(),
        Some(vec!["lookup".to_owned()]),
        "the statement reads the source's tools and no shortened list"
    );
    assert_eq!(
        ToolChoice::newest_in(&blocks)
            .expect("the thread carries the record")
            .names,
        vec!["lookup".to_owned()],
        "and the ledger fold agrees with it"
    );
}

/// AC14's other half — a source that recorded no choice hands none on.
/// Inventing one here would decide for a consumer that has not decided, and
/// the two answers are different: no record is every registered tool, an
/// empty record is none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compacted_thread_of_a_choiceless_source_records_no_choice() {
    let (ctx, _offered) = consumer_runtime().await;
    let store = ctx.store();
    let source = conversation(&ctx).await;
    let cut = store
        .compaction_cut(source)
        .await
        .unwrap()
        .expect("the ledger splits");

    let thread = store
        .open_compacted_thread(
            source,
            cut.first_half_ends,
            CompactedThread {
                ancestor_conversation_id: source,
                system_prompt: None,
                compaction_message: SUMMARY.into(),
                model: ModelOverride::default(),
            },
        )
        .await
        .unwrap();

    assert_eq!(store.newest_tool_choice(thread).await.unwrap(), None);
    assert!(
        store
            .list_blocks(thread)
            .await
            .unwrap()
            .iter()
            .all(|block| block.block_type != ToolChoice::KINDS[0]),
        "nothing invented a record the source never made"
    );
}
