//! AC13: a block kind defined OUTSIDE the library's own enum, composed through
//! the derive and proven end to end against the public API alone — it parses
//! from its stored row, loads through the descriptor path, its insert wakes a
//! tick, the frontier owes a turn, and a scripted provider answers it.
//!
//! This is the check the whole extension design exists for: nothing in here
//! reaches into the crate beyond what any consumer could write.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::{Value, json};

use agent_ledger::providers::{
    BoxFuture, ContentPart as WirePart, Message, MessageContent, ModelInfo, ProviderRx, ProviderTx,
    ToolDefinition, Usage,
};
use agent_ledger::store::ProviderInstance;
use agent_ledger::{
    Agency, Awaiting, Block, BlockKind, Column, ColumnType, ContentDescriptor, ContentPart,
    CoreEvent, DomainMigrations, EventBus, FromBlock, LeafKind, LlmError, Projection,
    ProviderModule, ProviderRegistry, ProviderRequest, ProviderResponse, Role, RuntimeContext,
    StopReason, Store, StoreConfig, StoreError, StreamEvent, ToolContext, ToolHandler, ToolOutcome,
    ToolRegistry, spawn_reactor,
};

// ─── The consumer's kind, exactly as a consumer would write it ───────────

/// A chat message of the consumer's own: a voice and a body, stored in a
/// content table the library has never heard of.
#[derive(Debug, Clone)]
struct ChatMessage {
    role: Option<Role>,
    body: String,
}

impl LeafKind for ChatMessage {
    const KINDS: &'static [&'static str] = &["chat_message"];

    const DESCRIPTORS: &'static [ContentDescriptor] = &[ContentDescriptor {
        table: "block_chat_message",
        domain: "chat",
        kinds: &["chat_message"],
        columns: &[
            Column::new("role", ColumnType::Text),
            Column::new("body", ColumnType::Text),
        ],
        reference_columns: &[],
        ephemeral: false,
        quoted_text_column: None,
    }];

    fn parse(block: &Block) -> Self {
        Self {
            role: block.role,
            body: block
                .fields
                .get("body")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }
    }
}

impl Agency for ChatMessage {
    fn awaiting(&self) -> Option<Awaiting> {
        (self.role == Some(Role::User)).then_some(Awaiting::Model)
    }
}

impl Projection for ChatMessage {
    fn group_role(&self) -> Option<Role> {
        self.role
    }

    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        Some(vec![ContentPart::Text {
            text: self.body.clone(),
        }])
    }

    fn llm_text(&self) -> Option<String> {
        Some(self.body.clone())
    }
}

/// The composed kind set: the library's kinds through the delegate, the
/// consumer's beside them — zero hand-written dispatch anywhere.
#[derive(Agency)]
enum AssistantKind {
    #[agency(delegate)]
    Core(BlockKind),
    ChatMessage(ChatMessage),
}

/// The migration that creates the consumer's table, in the shape the
/// descriptor contract asks for.
const CHAT_SCHEMA: &str = "
    CREATE TABLE block_chat_message (
        block_id INTEGER PRIMARY KEY REFERENCES blocks(id) ON DELETE CASCADE,
        role     TEXT,
        body     TEXT NOT NULL
    );";

fn chat_config() -> StoreConfig {
    StoreConfig {
        descriptors: AssistantKind::DESCRIPTORS,
        withdrawn_tables: &[],
        domain_migrations: vec![DomainMigrations {
            domain: "chat",
            sqls: vec![CHAT_SCHEMA],
        }],
    }
}

// ─── A scripted provider, wired through the real seams ───────────────────

const ANSWER: &str = "The plan is: answer the chat message.";

/// Answers every turn request with fixed prose, counts the turns, and keeps
/// each request's messages for the projection assertions. The metadata
/// worker's title derivation shares the binding and is the one request that
/// carries no tool definitions; it is answered and left out of the count.
struct ScriptedChat {
    turns: Arc<AtomicUsize>,
    seen: Arc<std::sync::Mutex<Vec<Vec<Message>>>>,
}

impl ProviderModule for ScriptedChat {
    fn type_id(&self) -> &'static str {
        "scripted-chat"
    }
    fn display_name(&self) -> &'static str {
        "Scripted chat"
    }
    fn description(&self) -> &'static str {
        "answers from a script"
    }
    fn get_config(&self, _provider_id: String) -> BoxFuture<'_, Result<Option<Value>, StoreError>> {
        Box::pin(async { Ok(Some(json!({}))) })
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
        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel();
        let (resp_tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();
        let turns = Arc::clone(&self.turns);
        let seen = Arc::clone(&self.seen);
        tokio::spawn(async move {
            while let Some(request) = req_rx.recv().await {
                let ProviderRequest::Stream {
                    messages, tools, ..
                } = request
                else {
                    continue;
                };
                if tools.is_empty() {
                    let _ = resp_tx.send(ProviderResponse::Event(StreamEvent::TextDelta {
                        text: "A derived title".into(),
                    }));
                    let _ = resp_tx.send(ProviderResponse::Done);
                    continue;
                }
                turns.fetch_add(1, Ordering::SeqCst);
                seen.lock().unwrap().push(messages);
                let _ = resp_tx.send(ProviderResponse::Event(StreamEvent::TextBlockStart));
                let _ = resp_tx.send(ProviderResponse::Event(StreamEvent::TextDelta {
                    text: ANSWER.into(),
                }));
                let _ = resp_tx.send(ProviderResponse::Event(StreamEvent::MessageEnd {
                    usage: Usage::default(),
                    stop_reason: StopReason::EndTurn,
                }));
            }
        });
        (req_tx, resp_rx)
    }
}

/// A tool that is never called. Registered so a turn request carries tool
/// definitions, which is what tells the script a turn from the metadata
/// worker's definition-free title derivation.
struct NoopTool;

impl ToolHandler<CoreEvent> for NoopTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "noop".into(),
            description: "does nothing".into(),
            parameters: json!({ "type": "object" }),
        }
    }
    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async { ToolOutcome::Done(String::new()) })
    }
}

// ─── The composed runtime, and the end-to-end proof ──────────────────────

/// Whether one neutral message carries the chat message's body — in either
/// content mode, because which one the fold picks is the projection's
/// decision, not this test's.
fn carries_body(message: &Message) -> bool {
    match &message.content {
        MessageContent::Text(text) => text.contains("What is the plan?"),
        MessageContent::Parts(parts) => parts.iter().any(
            |part| matches!(part, WirePart::Text { text } if text.contains("What is the plan?")),
        ),
    }
}

/// Poll the ledger until `accept` says it has the shape awaited, with a
/// deadline so a stall is a named failure instead of a hung suite.
async fn await_ledger(
    store: &Store,
    conv: i64,
    what: &str,
    accept: impl Fn(&[Block]) -> bool,
) -> Vec<Block> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let blocks = store.list_blocks(conv).await.unwrap();
        if accept(&blocks) {
            return blocks;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out awaiting {what}; ledger: {:?}",
            blocks
                .iter()
                .map(|b| b.block_type.as_str())
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The wake, proven on the bus: the append put the consumer kind at the
/// frontier, and the runtime's own conversation state must say so — work due,
/// awaiting a model turn, exactly what the kind's `awaiting()` answers —
/// BEFORE the turn's stream completes. The bus is ordered per subscriber, so
/// the order of the two events IS the proof: a state event arriving only
/// after `StreamDone` would mean the runtime never derived the owed turn from
/// the consumer kind, just answered and reconciled.
async fn assert_wake_precedes_turn_end(
    events: &mut tokio::sync::broadcast::Receiver<CoreEvent>,
    conv: i64,
) {
    let deadline = Duration::from_secs(10);
    let mut woke = false;
    let mut turn_done = false;
    while !(woke && turn_done) {
        let event = tokio::time::timeout(deadline, events.recv())
            .await
            .expect("the owed-turn state and the stream end both reach the bus")
            .expect("the bus outlives the test");
        match event {
            CoreEvent::ConversationState {
                conversation_id,
                latched: false,
                work_due: true,
                awaiting: Some(Awaiting::Model),
            } if conversation_id == conv => woke = true,
            CoreEvent::StreamDone {
                conversation_id, ..
            } if conversation_id == conv => {
                assert!(
                    woke,
                    "the owed-turn state (work_due, awaiting Model) must be observable \
                     on the bus before the turn's stream completes"
                );
                turn_done = true;
            }
            _ => {}
        }
    }
}

/// The derive's composition, pinned without a runtime: the parse chain
/// resolves the consumer's kind through its own parse and everything else
/// through the delegate, and the concatenated descriptor set is exactly the
/// leaf's declaration.
#[test]
fn the_composed_enum_parses_and_concatenates_descriptors() {
    let stored = Block {
        id: 7,
        role: Some(Role::User),
        block_type: "chat_message".into(),
        created_at: String::new(),
        dispatch_anchor: None,
        fields: {
            let mut fields = serde_json::Map::new();
            fields.insert("body".into(), json!("hello"));
            fields
        },
    };
    match AssistantKind::from_block(&stored) {
        AssistantKind::ChatMessage(message) => {
            assert_eq!(message.body, "hello");
            assert_eq!(message.role, Some(Role::User));
        }
        AssistantKind::Core(_) => panic!("the consumer kind resolved through the delegate"),
    }

    let core = Block {
        block_type: "text".into(),
        ..stored
    };
    assert!(
        matches!(
            AssistantKind::from_block(&core),
            AssistantKind::Core(BlockKind::Text(_))
        ),
        "a library kind resolves through the delegate, untouched"
    );

    assert_eq!(AssistantKind::DESCRIPTORS.len(), 1);
    assert_eq!(AssistantKind::DESCRIPTORS[0].table, "block_chat_message");
    agent_ledger::agency::check_descriptor_durability::<AssistantKind>(AssistantKind::DESCRIPTORS)
        .expect("the derive keeps durable() and the descriptor's ephemerality one fact");
}

/// AC13 end to end: the composed runtime is spawned over the configured
/// store, one `chat_message` row is appended through the consumer write path,
/// and everything after that is the machinery acting on the consumer kind's
/// own answers — the insert wakes a tick, the frontier owes a turn because
/// the kind says so, the projection carries its body to the provider, and the
/// scripted answer is ingested back into the ledger.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_consumer_kind_parses_loads_wakes_and_takes_a_turn() {
    let store = Store::in_memory_with(chat_config()).unwrap();
    assert!(
        store.content_tables().contains(&"block_chat_message"),
        "the descriptor's table joins the one content-table list"
    );

    store
        .save_provider_instance(ProviderInstance {
            id: "chat-1".into(),
            provider_type: "scripted-chat".into(),
            name: "Scripted chat".into(),
        })
        .await
        .unwrap();
    let conv = store
        .create_conversation(
            "chat-1".into(),
            "model-x".into(),
            "Model X".into(),
            "scripted-chat".into(),
        )
        .await
        .unwrap();
    // The prompt is the conversation's first block, and the dispatch refuses
    // a ledger that opens with anything else — so a conversation a turn can
    // be dispatched for starts here, with the consumer's own words.
    store
        .insert_system_prompt(conv, "answer the member".into())
        .await
        .unwrap();

    let turns = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut providers = ProviderRegistry::new();
    providers.register(Box::new(ScriptedChat {
        turns: Arc::clone(&turns),
        seen: Arc::clone(&seen),
    }));
    let mut tools = ToolRegistry::new();
    tools.register("noop", NoopTool);

    let ctx: RuntimeContext<AssistantKind, CoreEvent> = RuntimeContext::new(
        store,
        Arc::new(EventBus::<CoreEvent>::new()),
        Arc::new(providers),
        Arc::new(tools),
    );
    spawn_reactor(ctx.clone());

    // Subscribed BEFORE the append so the whole event order is observed: the
    // wake must be provable from the bus, not narrated.
    let mut events = ctx.bus().subscribe();

    // A conversation starts latched until something proactive releases it —
    // and the bus is the runtime's one control surface, so the release is an
    // event, exactly as a consumer's own ingress would send it.
    ctx.bus().emit(CoreEvent::UnlatchRequested {
        conversation_id: conv,
    });

    // The consumer write path: one transactional header + junction + content
    // row, driven by the descriptor. Its change announcement is the wake.
    let mut fields = serde_json::Map::new();
    fields.insert("body".into(), json!("What is the plan?"));
    let appended = ctx
        .store()
        .append_consumer_block(conv, Some(Role::User), "chat_message", fields, None)
        .await
        .unwrap();

    // The wake, proven on the bus rather than narrated: see
    // `assert_wake_precedes_turn_end`.
    assert_wake_precedes_turn_end(&mut events, conv).await;

    // The owed turn, answered: the ledger settles as the prompt it opened
    // with, the day's date marker — tripped by the user-voiced append inside
    // its own transaction — then the stored chat message, then the scripted
    // prose.
    let blocks = await_ledger(ctx.store(), conv, "the answered turn", |blocks| {
        blocks.len() == 4
            && blocks
                .last()
                .is_some_and(|b| b.block_type == "text" && b.fields["content"] == json!(ANSWER))
    })
    .await;
    assert_eq!(
        blocks[0].block_type, "system_prompt",
        "the prompt is the head of the ledger"
    );
    assert_eq!(
        blocks[1].block_type, "date_marker",
        "the marker precedes the message that owes the turn"
    );

    // Loaded through the descriptor path: the row comes back with the role
    // and the declared column read by name into its fields.
    assert_eq!(blocks[2].id, appended);
    assert_eq!(blocks[2].block_type, "chat_message");
    assert_eq!(blocks[2].role, Some(Role::User));
    assert_eq!(blocks[2].fields["body"], json!("What is the plan?"));

    // Parsed from its stored row, through the derive's chain.
    match AssistantKind::from_block(&blocks[2]) {
        AssistantKind::ChatMessage(message) => {
            assert_eq!(message.body, "What is the plan?");
            assert_eq!(message.awaiting(), Some(Awaiting::Model));
        }
        AssistantKind::Core(_) => panic!("the stored row resolved through the delegate"),
    }

    // Exactly one turn was owed and fired, and its request carried the
    // consumer kind's body — the kind's own Projection, folded by the
    // machinery, reached the model.
    assert_eq!(
        turns.load(Ordering::SeqCst),
        1,
        "one owed turn, one request"
    );
    let requests = seen.lock().unwrap();
    assert!(
        requests[0].iter().any(carries_body),
        "the projected messages carry the chat message's body: {requests:?}"
    );
}
