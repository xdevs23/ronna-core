//! Delegation completeness, pinned instead of trusted: the derive's generated
//! dispatch is a hand-maintained mirror of two traits' hooks, and a mirror
//! drifts the day a trait gains a hook the generator does not know. This test
//! is the tripwire. A probe kind overrides EVERY `Agency` and every
//! `Projection` hook with an answer distinct from that hook's default, the
//! probe is composed through the derive, and the composed enum must return
//! the leaf's answer for each — a hook the derive fails to delegate answers
//! with the trait default instead and goes red here.
//!
//! The loop is closed mechanically: the two probe impls deny
//! `clippy::missing_trait_methods`, so a hook added to either trait fails the
//! lint until it is overridden here, and overriding it here with a distinct
//! answer keeps this test red until the generator delegates it too.

use std::sync::Arc;

use agent_ledger::{
    Agency, AgencyCtx, Awaiting, Block, BlockKind, ContentPart, CoreEvent, EventBus, FromBlock,
    GateDecision, LeafKind, Projection, Role, RuntimeEvent, Store, StoreError,
};

/// The probe: a kind whose every hook answers something no default answers.
#[derive(Debug, Clone)]
struct Probe;

impl LeafKind for Probe {
    const KINDS: &'static [&'static str] = &["probe"];

    fn parse(_: &Block) -> Self {
        Self
    }
}

#[deny(clippy::missing_trait_methods)]
impl Agency for Probe {
    fn awaiting(&self) -> Option<Awaiting> {
        Some(Awaiting::OutOfBand)
    }

    fn durable(&self) -> bool {
        false
    }

    async fn gate<E: RuntimeEvent>(&self, _ctx: &AgencyCtx<E>) -> GateDecision {
        GateDecision::Refuse {
            reason: "probe-gate".into(),
        }
    }

    async fn run<E: RuntimeEvent>(&self, _ctx: &AgencyCtx<E>) -> Result<bool, StoreError> {
        Ok(false)
    }

    fn post_gate_id(&self, _ledger: &[Block]) -> Option<i64> {
        Some(41)
    }

    async fn run_post_gate<E: RuntimeEvent>(&self, _ctx: &AgencyCtx<E>) -> Result<(), StoreError> {
        Err(StoreError::Other("probe-post-gate".into()))
    }
}

#[deny(clippy::missing_trait_methods)]
impl Projection for Probe {
    fn group_role(&self) -> Option<Role> {
        Some(Role::Tool)
    }

    fn llm_parts(&self) -> Option<Vec<ContentPart>> {
        Some(vec![ContentPart::Text {
            text: "probe-parts".into(),
        }])
    }

    fn llm_text(&self) -> Option<String> {
        Some("probe-text".into())
    }

    fn forces_parts(&self) -> bool {
        true
    }
}

/// The probe, composed exactly as a consumer composes.
#[derive(Agency)]
enum ProbedKind {
    #[agency(delegate)]
    Core(BlockKind),
    Probe(Probe),
}

/// Every hook of both traits, asserted through the composed enum against the
/// probe's own distinct answer. The expected values are spelled out rather
/// than read off a `Probe` instance so a probe whose override quietly
/// regressed to the default cannot vouch for itself.
#[tokio::test]
async fn the_derived_enum_returns_the_leaf_answer_for_every_hook() {
    let composed = ProbedKind::Probe(Probe);
    let ctx: AgencyCtx<CoreEvent> = AgencyCtx {
        conversation_id: 1,
        store: Store::in_memory().expect("an in-memory store"),
        bus: Arc::new(EventBus::new()),
    };

    // Agency, all six hooks.
    assert_eq!(composed.awaiting(), Some(Awaiting::OutOfBand));
    assert!(!composed.durable(), "durable() is the leaf's own answer");
    assert_eq!(
        composed.gate(&ctx).await,
        GateDecision::Refuse {
            reason: "probe-gate".into()
        }
    );
    assert!(
        !composed.run(&ctx).await.expect("the probe's run succeeds"),
        "run() carries the leaf's not-done answer"
    );
    assert_eq!(composed.post_gate_id(&[]), Some(41));
    match composed.run_post_gate(&ctx).await {
        Err(StoreError::Other(reason)) => assert_eq!(reason, "probe-post-gate"),
        other => panic!("run_post_gate() did not carry the leaf's answer: {other:?}"),
    }

    // Projection, all four hooks.
    assert_eq!(composed.group_role(), Some(Role::Tool));
    assert_eq!(
        composed.llm_parts(),
        Some(vec![ContentPart::Text {
            text: "probe-parts".into()
        }])
    );
    assert_eq!(composed.llm_text(), Some("probe-text".to_string()));
    assert!(composed.forces_parts());

    // And the chain resolves the probe's stored string to the probe.
    let block = Block {
        id: 1,
        role: None,
        block_type: "probe".into(),
        created_at: String::new(),
        dispatch_anchor: None,
        fields: serde_json::Map::new(),
    };
    assert!(matches!(
        ProbedKind::from_block(&block),
        ProbedKind::Probe(_)
    ));
}
