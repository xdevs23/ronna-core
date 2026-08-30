//! AC6 and the byte pin: a tool's own window, bound the way a consumer binds
//! one — from outside the crate, through the public builder alone.
//!
//! Two things are provable only from here. The first is PUBLICITY: that this
//! file compiles is the proof `with_tool_window` is reachable from a consumer,
//! which no in-crate test can establish. The second is the refusal's exact
//! bytes under a REAL tool name: the library's own sources name no tool a
//! product would have, so the sentence a model actually reads is pinned here,
//! where the name belongs to the consumer this test plays.
//!
//! The bound this file's consumer sets is a plausible one — `lookup_release`
//! at six calls per sixty seconds, the allowance an embedder gives a lookup
//! it does not want a model leaning on. Nothing in the library ships that
//! name or those numbers; they arrive here the way they arrive in an
//! embedder, one builder call.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use agent_ledger::providers::{BoxFuture, ToolDefinition};
use agent_ledger::{
    BlockKind, CallOrigin, CoreEvent, EventBus, ProviderRegistry, RuntimeContext, Store,
    ToolContext, ToolHandler, ToolOutcome, ToolRegistry,
};

/// The consumer's failing lookup, reduced to what the window cares about: it
/// counts the bodies that ran, because a refused call must never reach one.
struct LookupRelease {
    runs: Arc<AtomicUsize>,
}

impl ToolHandler<CoreEvent> for LookupRelease {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "lookup_release".into(),
            description: "looks a release up".into(),
            parameters: serde_json::json!({ "type": "object" }),
        }
    }

    fn execute<'a>(
        &'a self,
        _input: &'a str,
        _ctx: ToolContext<'a, CoreEvent>,
    ) -> BoxFuture<'a, ToolOutcome> {
        Box::pin(async move {
            self.runs.fetch_add(1, Ordering::SeqCst);
            ToolOutcome::Done("release 1.0".into())
        })
    }
}

/// A runtime a consumer could have built: a store, a bus, no providers, and
/// one tool. The conversation it will spend is created with it.
async fn consumer_runtime() -> (RuntimeContext<BlockKind, CoreEvent>, i64, Arc<AtomicUsize>) {
    let store = Store::in_memory().unwrap();
    let conversation = store
        .create_conversation("p1".into(), "model".into(), "Model".into(), String::new())
        .await
        .unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(
        "lookup_release",
        LookupRelease {
            runs: Arc::clone(&runs),
        },
    );
    let ctx = RuntimeContext::new(
        store,
        Arc::new(EventBus::<CoreEvent>::new()),
        Arc::new(ProviderRegistry::new()),
        Arc::new(tools),
    );
    (ctx, conversation, runs)
}

/// Record one call of the consumer's tool and hand the runner its wakeup —
/// the pair every round below is made of.
async fn one_round(ctx: &RuntimeContext<BlockKind, CoreEvent>, conversation: i64, id: &str) {
    let agency = ctx.agency(conversation);
    let call = ctx
        .runner()
        .insert_call(
            &agency,
            false,
            id.to_owned(),
            "lookup_release".into(),
            "{}".into(),
            CallOrigin::default(),
        )
        .await
        .unwrap();
    ctx.runner().run_wakeup(&agency, false, call).await;
}

/// The conversation's recorded tool errors, in ledger order.
async fn error_texts(ctx: &RuntimeContext<BlockKind, CoreEvent>, conversation: i64) -> Vec<String> {
    ctx.store()
        .list_blocks(conversation)
        .await
        .unwrap()
        .into_iter()
        .filter(|block| block.block_type == "tool_error")
        .map(|block| block.fields["error"].as_str().unwrap().to_owned())
        .collect()
}

/// AC6 and the byte pin. The bound is written through the PUBLIC builder at
/// construction, six calls of `lookup_release` run, and the seventh inside the
/// same minute is refused — its body never reached — with the sentence pinned
/// byte for byte, because the model reads it and re-plans against it.
#[tokio::test]
async fn the_public_builder_binds_a_tool_and_its_refusal_is_pinned() {
    let (ctx, conversation, runs) = consumer_runtime().await;
    let ctx = ctx.with_tool_window("lookup_release", 6, 60);

    for round in 0..7 {
        one_round(&ctx, conversation, &format!("lr-{round}")).await;
    }

    assert_eq!(
        runs.load(Ordering::SeqCst),
        6,
        "the bound's own calls ran, and the seventh never reached the body"
    );
    assert_eq!(
        error_texts(&ctx, conversation).await,
        vec![
            "tool-call rate limit: this conversation has spent its 6 lookup_release calls for \
             the last 60 seconds, and this call was not run. Answer with what you already \
             have, or use a different tool, or wait before calling this one again."
                .to_owned()
        ],
        "the refusal is recorded as the call's outcome, naming the tool and its numbers"
    );
}

/// AC6's ordering requirement, made observable: the builder writes through the
/// runner's SOLE reference, so a context already shared — here by one clone —
/// panics loudly rather than writing a window half the runtime would never
/// see. The clone's existence is what shares the runner; nothing needs it
/// dropped. Configure every window before sharing.
#[tokio::test]
#[should_panic(expected = "a tool's window is set while the context still owns its runner alone")]
async fn binding_a_tool_window_after_the_context_is_shared_panics() {
    let (ctx, _conversation, _runs) = consumer_runtime().await;
    let _shared = ctx.clone();
    let _bound = ctx.with_tool_window("lookup_release", 6, 60);
}

/// The builder binds only a name the registry answers to: a typo'd tool name
/// would otherwise compile, pass and silently protect nothing, so it fails at
/// construction instead, naming the name — the registry is the whole set of
/// names a call can resolve against.
#[tokio::test]
#[should_panic(expected = "a tool's window is bound to a name nothing registered")]
async fn binding_a_tool_window_to_an_unregistered_name_panics() {
    let (ctx, _conversation, _runs) = consumer_runtime().await;
    let _bound = ctx.with_tool_window("lookup_relese", 6, 60);
}

/// The span must be positive: a zero span is a construction-time
/// misconfiguration — no span, no window — and dies loudly at the builder.
#[tokio::test]
#[should_panic(expected = "a tool's window needs a positive span")]
async fn binding_a_tool_window_with_no_span_panics() {
    let (ctx, _conversation, _runs) = consumer_runtime().await;
    let _bound = ctx.with_tool_window("lookup_release", 6, 0);
}

/// The builder's replace promise, pinned: a second call for the same name
/// REPLACES that tool's bound — one entry per name, and the later call wins.
/// Six bound first, then one: under the first bound every round below would
/// run, under the second only the first does, and the refusals name the
/// SECOND bound's numbers.
#[tokio::test]
async fn a_second_bound_for_the_same_tool_replaces_the_first() {
    let (ctx, conversation, runs) = consumer_runtime().await;
    let ctx = ctx
        .with_tool_window("lookup_release", 6, 60)
        .with_tool_window("lookup_release", 1, 60);

    for round in 0..3 {
        one_round(&ctx, conversation, &format!("lr-{round}")).await;
    }

    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the SECOND bound governs — one call ran, not the first bound's six"
    );
    assert_eq!(
        error_texts(&ctx, conversation).await,
        vec![
            "tool-call rate limit: this conversation has spent its 1 lookup_release calls for \
             the last 60 seconds, and this call was not run. Answer with what you already \
             have, or use a different tool, or wait before calling this one again."
                .to_owned(),
            "tool-call rate limit: this conversation has spent its 1 lookup_release calls for \
             the last 60 seconds, and this call was not run. Answer with what you already \
             have, or use a different tool, or wait before calling this one again."
                .to_owned(),
        ],
        "both calls past the replaced bound are refused in ITS numbers, never the first's"
    );
}

/// A bound of ZERO calls is legal and refuses the tool's very first call: the
/// count includes the call under admission, so one is already past a zero
/// bound. The body never runs, and the refusal names the zero bound itself.
#[tokio::test]
async fn a_zero_call_bound_refuses_the_very_first_call() {
    let (ctx, conversation, runs) = consumer_runtime().await;
    let ctx = ctx.with_tool_window("lookup_release", 0, 60);

    one_round(&ctx, conversation, "lr-first").await;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "the very first call never reached the body"
    );
    assert_eq!(
        error_texts(&ctx, conversation).await,
        vec![
            "tool-call rate limit: this conversation has spent its 0 lookup_release calls for \
             the last 60 seconds, and this call was not run. Answer with what you already \
             have, or use a different tool, or wait before calling this one again."
                .to_owned()
        ],
        "the refusal names the zero bound, and the model learns the tool is closed"
    );
}
