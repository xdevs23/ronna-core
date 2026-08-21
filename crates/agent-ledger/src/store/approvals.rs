//! The approval chain's two block kinds: the request a gated call parks on,
//! and the decision that resolves it.

use rusqlite::{OptionalExtension, params};

use crate::types::{ApprovalChoice, denial_error_text};

use super::messages::insert_block;
use super::tool_calls::call_resolution_exists;
use super::{Store, StoreError, transact};

impl Store {
    /// Append the approval request covering `for_block_id` — the human's block
    /// (role user), inserted by the runner's chokepoint when a gated tool
    /// defers. Insert-first is satisfied by construction: the covered call is
    /// already in the ledger, so `for_block_id` is its REAL id, never a
    /// predicted one.
    ///
    /// Conditional, in its own transaction, the same shape as the decision
    /// write: the insert happens only where no request in this conversation
    /// covers the call yet. The fold over the ledger only ever consults the
    /// first covering request, so a second one would park the call behind a
    /// question whose answer nothing reads. Answers `Some(block_id)` when this
    /// write parked the call and `None` when a request already covered it.
    ///
    /// # Errors
    ///
    /// If `for_block_id` is not a tool call in this conversation — a request
    /// can only cover a call, and accepting anything else here is what used to
    /// surface later as a bare no-rows error on the denial path — or if the
    /// insert fails or the store's actor has stopped.
    pub async fn insert_approval_request_block(
        &self,
        conversation_id: i64,
        for_block_id: i64,
    ) -> Result<Option<i64>, StoreError> {
        self.run(move |conn| {
            transact(conn, |tx| {
                let covers_a_call: bool = tx.query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM block_tool_call btc
                         JOIN conversation_blocks cb ON cb.block_id = btc.block_id
                         WHERE btc.block_id = ?1 AND cb.conversation_id = ?2)",
                    params![for_block_id, conversation_id],
                    |row| row.get(0),
                )?;
                if !covers_a_call {
                    return Err(StoreError::Other(format!(
                        "block {for_block_id} is not a tool call in conversation \
                         {conversation_id}: an approval request can only cover a call"
                    )));
                }
                let already_covered: bool = tx.query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM block_approval_request bar
                         JOIN conversation_blocks cb ON cb.block_id = bar.block_id
                         WHERE bar.for_block_id = ?1 AND cb.conversation_id = ?2)",
                    params![for_block_id, conversation_id],
                    |row| row.get(0),
                )?;
                if already_covered {
                    return Ok(None);
                }
                let block_id = insert_block(tx, conversation_id, "approval_request")?;
                tx.execute(
                    "INSERT INTO block_approval_request (block_id, for_block_id) VALUES (?1, ?2)",
                    params![block_id, for_block_id],
                )?;
                Ok(Some(block_id))
            })
        })
        .await
    }

    /// Append-if-undecided: the decision INSERT is conditional on no decision
    /// block already referencing the same request — block storage stays
    /// append-only, and the loser of a submit race gets a clean error, never a
    /// second decision. A denial resolves the covered call with its tool error
    /// in the SAME transaction, so no crash window can leave a
    /// denied-but-unresolved call parked forever — and the error's sentence is
    /// built HERE, from the reasons, by the one constructor
    /// ([`denial_error_text`]): the write is the only construction site, so a
    /// second caller can never record a divergent sentence.
    ///
    /// A denial of an ALREADY-RESOLVED call is refused whole, recording
    /// nothing: the call's outcome is on the ledger, and appending a second
    /// one would hand the model two answers to one call.
    ///
    /// # Errors
    ///
    /// If `request_block_id` is not an approval request in this conversation,
    /// if the request is already decided, if the denial's covered call is not a
    /// tool call or is already resolved, if the transaction fails, or if the
    /// store's actor has stopped.
    pub async fn insert_approval_decision_block(
        &self,
        conversation_id: i64,
        request_block_id: i64,
        decision: ApprovalChoice,
        system_reason: Option<String>,
        user_reason: Option<String>,
    ) -> Result<i64, StoreError> {
        let denial_error = matches!(decision, ApprovalChoice::Denied)
            .then(|| denial_error_text(system_reason.as_deref(), user_reason.as_deref()));
        self.run(move |conn| {
            let tx = conn.transaction()?;

            let call_block_id: i64 = tx
                .query_row(
                    "SELECT bar.for_block_id
                     FROM block_approval_request bar
                     JOIN blocks b ON b.id = bar.block_id
                     JOIN conversation_blocks cb ON cb.block_id = bar.block_id
                     WHERE bar.block_id = ?1
                       AND b.block_type = 'approval_request'
                       AND cb.conversation_id = ?2",
                    params![request_block_id, conversation_id],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(|| {
                    StoreError::Other(format!(
                        "approval request {request_block_id} not found in conversation {conversation_id}"
                    ))
                })?;

            // Conversation-local, like every approval read — decisions are read
            // off the local ledger: a junction-shared request is decided per
            // conversation, so deciding it in a fork never makes the source's
            // copy undecidable, or the other way round.
            let decided: bool = tx.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM block_approval_decision bad
                     JOIN conversation_blocks cb ON cb.block_id = bad.block_id
                     WHERE bad.for_block_id = ?1 AND cb.conversation_id = ?2)",
                params![request_block_id, conversation_id],
                |row| row.get(0),
            )?;
            if decided {
                return Err(StoreError::Other(format!(
                    "approval request {request_block_id} is already decided"
                )));
            }

            // A denial's covered call is vetted BEFORE anything is inserted, so
            // a refused denial rolls back to exactly the ledger it found:
            // no decision, no error, nothing. Resolution is keyed on the CALL
            // BLOCK ID, never on the provider's tool_call_id — the model may
            // reuse that id, and a sibling call's outcome must not refuse this
            // call's denial.
            let denied_call: Option<String> = denial_error
                .as_ref()
                .map(|_| {
                    let tool_call_id: String = tx
                        .query_row(
                            "SELECT tool_call_id FROM block_tool_call WHERE block_id = ?1",
                            [call_block_id],
                            |row| row.get(0),
                        )
                        .optional()?
                        .ok_or_else(|| {
                            StoreError::Other(format!(
                                "approval request {request_block_id} covers block \
                                 {call_block_id}, which is not a tool call"
                            ))
                        })?;
                    if call_resolution_exists(&tx, conversation_id, call_block_id)? {
                        return Err(StoreError::Other(format!(
                            "tool call '{tool_call_id}' (block {call_block_id}) is already \
                             resolved in conversation {conversation_id}: the denial conflicts \
                             with the recorded outcome and records nothing"
                        )));
                    }
                    Ok(tool_call_id)
                })
                .transpose()?;

            let block_id = insert_block(&tx, conversation_id, "approval_decision")?;
            tx.execute(
                "INSERT INTO block_approval_decision
                     (block_id, for_block_id, decision, system_reason, user_reason)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    block_id,
                    request_block_id,
                    decision.as_str(),
                    system_reason,
                    user_reason
                ],
            )?;

            if let (Some(error), Some(tool_call_id)) = (denial_error, denied_call) {
                let error_block_id = insert_block(&tx, conversation_id, "tool_error")?;
                tx.execute(
                    "INSERT INTO block_tool_error (block_id, tool_call_id, error, source_block_id)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![error_block_id, tool_call_id, error, call_block_id],
                )?;
            }

            tx.commit()?;
            Ok(block_id)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use crate::block::Role;
    use crate::store::{Store, ToolCallInsert};
    use crate::types::ApprovalChoice;

    async fn fixture() -> (Store, i64, i64) {
        let store = Store::in_memory().unwrap();
        let conv = store
            .create_conversation("p".into(), "m".into(), "m".into(), String::new())
            .await
            .unwrap();
        let call = store
            .insert_tool_call_block(
                conv,
                Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call_1".into(),
                    name: "danger".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        (store, conv, call)
    }

    /// Both approval kinds round-trip through the store with role user and
    /// their structural fields intact. Role user is mechanical, not cosmetic:
    /// the fork's group walk reads the RAW role, so role user is what keeps
    /// approval blocks inside the surrounding user turn's group boundary.
    #[tokio::test]
    async fn approval_blocks_round_trip_with_user_role() {
        let (store, conv, call) = fixture().await;
        let request = store
            .insert_approval_request_block(conv, call)
            .await
            .unwrap()
            .expect("the first request writes");
        let decision = store
            .insert_approval_decision_block(
                conv,
                request,
                ApprovalChoice::Approved,
                None,
                Some("looks safe".into()),
            )
            .await
            .unwrap();

        let blocks = store.list_blocks(conv).await.unwrap();
        let req = blocks.iter().find(|b| b.id == request).unwrap();
        assert_eq!(req.block_type, "approval_request");
        assert_eq!(req.role, Some(Role::User));
        assert_eq!(req.fields["for_block_id"], Value::from(call));

        let dec = blocks.iter().find(|b| b.id == decision).unwrap();
        assert_eq!(dec.block_type, "approval_decision");
        assert_eq!(dec.role, Some(Role::User));
        assert_eq!(dec.fields["for_block_id"], Value::from(request));
        assert_eq!(dec.fields["decision"], Value::from("approved"));
        assert_eq!(dec.fields["system_reason"], Value::Null);
        assert_eq!(dec.fields["user_reason"], Value::from("looks safe"));
    }

    /// The conditional append: a second decision on the same request fails
    /// cleanly and appends NOTHING — not a decision, and (for a losing denial)
    /// not the call's error either.
    #[tokio::test]
    async fn second_decision_write_fails_cleanly() {
        let (store, conv, call) = fixture().await;
        let request = store
            .insert_approval_request_block(conv, call)
            .await
            .unwrap()
            .expect("the first request writes");
        store
            .insert_approval_decision_block(conv, request, ApprovalChoice::Approved, None, None)
            .await
            .unwrap();

        let before = store.list_blocks(conv).await.unwrap().len();
        let lost = store
            .insert_approval_decision_block(
                conv,
                request,
                ApprovalChoice::Denied,
                None,
                Some("changed my mind".into()),
            )
            .await;
        assert!(lost.is_err(), "the loser of the race gets a clean error");

        let blocks = store.list_blocks(conv).await.unwrap();
        assert_eq!(blocks.len(), before, "the losing write appends nothing");
        assert_eq!(
            blocks
                .iter()
                .filter(|b| b.block_type == "approval_decision")
                .count(),
            1
        );
        assert!(blocks.iter().all(|b| b.block_type != "tool_error"));
    }

    /// A denial lands the decision AND the covered call's tool error in one
    /// transaction — no crash window can observe a denied-but-unresolved call.
    #[tokio::test]
    async fn denial_resolves_the_call_atomically() {
        let (store, conv, call) = fixture().await;
        let request = store
            .insert_approval_request_block(conv, call)
            .await
            .unwrap()
            .expect("the first request writes");
        store
            .insert_approval_decision_block(
                conv,
                request,
                ApprovalChoice::Denied,
                Some("policy".into()),
                None,
            )
            .await
            .unwrap();

        let blocks = store.list_blocks(conv).await.unwrap();
        let error = blocks
            .iter()
            .find(|b| b.block_type == "tool_error")
            .expect("error landed");
        assert_eq!(error.fields["tool_call_id"], Value::from("call_1"));
        assert!(
            error.fields["error"].as_str().unwrap().contains("policy"),
            "the model learns who denied and why"
        );
        let positions: Vec<usize> = blocks
            .iter()
            .enumerate()
            .filter(|(_, b)| matches!(b.block_type.as_str(), "approval_decision" | "tool_error"))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(positions.len(), 2, "decision and error landed together");
    }

    /// A junction-shared request is decidable PER CONVERSATION, following the
    /// local-ledger discipline: deciding it in a fork leaves the source's copy
    /// open — and each ledger ends up with exactly one decision.
    #[tokio::test]
    async fn junction_shared_request_is_decidable_per_conversation() {
        use crate::store::{Continuation, ModelOverride};
        use crate::types::InputBlock;

        let (store, conv, call) = fixture().await;
        let request = store
            .insert_approval_request_block(conv, call)
            .await
            .unwrap()
            .expect("the first request writes");
        let anchor = store
            .insert_user_blocks(
                conv,
                vec![InputBlock::Text {
                    content: "carry on".into(),
                }],
            )
            .await
            .unwrap()[0];
        let fork = store
            .fork_continuation(conv, anchor, Continuation::Rerun, ModelOverride::default())
            .await
            .unwrap();

        // The fork decides first — the source's copy must stay decidable.
        store
            .insert_approval_decision_block(fork, request, ApprovalChoice::Approved, None, None)
            .await
            .unwrap();
        store
            .insert_approval_decision_block(
                conv,
                request,
                ApprovalChoice::Denied,
                None,
                Some("not in this thread".into()),
            )
            .await
            .unwrap();

        for (conversation, expected) in [(fork, "approved"), (conv, "denied")] {
            let decisions: Vec<_> = store
                .list_blocks(conversation)
                .await
                .unwrap()
                .into_iter()
                .filter(|b| b.block_type == "approval_decision")
                .collect();
            assert_eq!(decisions.len(), 1, "exactly one decision per conversation");
            assert_eq!(decisions[0].fields["decision"], Value::from(expected));
        }

        // Each side stays append-once: a second decision in the fork loses.
        assert!(
            store
                .insert_approval_decision_block(fork, request, ApprovalChoice::Denied, None, None)
                .await
                .is_err()
        );
    }

    /// The decision validates its request: a missing id and a non-request block
    /// are both rejected.
    #[tokio::test]
    async fn decision_requires_a_real_request() {
        let (store, conv, call) = fixture().await;
        assert!(
            store
                .insert_approval_decision_block(conv, 999_999, ApprovalChoice::Approved, None, None)
                .await
                .is_err()
        );
        assert!(
            store
                .insert_approval_decision_block(conv, call, ApprovalChoice::Approved, None, None)
                .await
                .is_err(),
            "a tool_call block is not an approval request"
        );
    }

    /// The conditional request write: a second request covering the same call
    /// answers `None` and appends nothing. The fold only ever consults the
    /// first covering request, so a second one would park the call behind a
    /// question whose answer nothing reads.
    #[tokio::test]
    async fn a_covered_call_refuses_a_second_request() {
        let (store, conv, call) = fixture().await;
        store
            .insert_approval_request_block(conv, call)
            .await
            .unwrap()
            .expect("the first request writes");
        let before = store.list_blocks(conv).await.unwrap().len();

        let second = store
            .insert_approval_request_block(conv, call)
            .await
            .unwrap();
        assert!(second.is_none(), "a covered call takes no second request");

        let blocks = store.list_blocks(conv).await.unwrap();
        assert_eq!(blocks.len(), before, "the losing write appends nothing");
        assert_eq!(
            blocks
                .iter()
                .filter(|b| b.block_type == "approval_request")
                .count(),
            1
        );
    }

    /// A request can only cover a tool call: a text block and a missing id are
    /// both refused at insert, with the block named — not accepted here to
    /// surface later as a bare no-rows error on the denial path.
    #[tokio::test]
    async fn a_request_over_a_non_call_is_refused_at_insert() {
        let (store, conv, _call) = fixture().await;
        let text = store
            .insert_text_block(conv, Role::User, "not a call".into())
            .await
            .unwrap();

        for bogus in [text, 999_999] {
            let refused = store.insert_approval_request_block(conv, bogus).await;
            match refused {
                Err(crate::store::StoreError::Other(message)) => {
                    assert!(
                        message.contains(&bogus.to_string()) && message.contains("not a tool call"),
                        "the refusal names the block: {message}"
                    );
                }
                other => panic!("expected the named refusal, got {other:?}"),
            }
        }
        assert!(
            store
                .list_blocks(conv)
                .await
                .unwrap()
                .iter()
                .all(|b| b.block_type != "approval_request"),
            "nothing was appended"
        );
    }

    /// A denial that arrives AFTER the call resolved is refused whole — a
    /// conflict error, no decision, no error block: the call's recorded outcome
    /// stays its only one, so no vendor ever sees two results for one id.
    #[tokio::test]
    async fn a_denial_after_resolution_is_refused_and_appends_nothing() {
        let (store, conv, call) = fixture().await;
        let request = store
            .insert_approval_request_block(conv, call)
            .await
            .unwrap()
            .expect("the first request writes");
        store
            .complete_tool_call_block(conv, "call_1".into(), "already done".into(), call)
            .await
            .unwrap()
            .expect("the resolution writes");
        let before = store.list_blocks(conv).await.unwrap().len();

        let refused = store
            .insert_approval_decision_block(
                conv,
                request,
                ApprovalChoice::Denied,
                None,
                Some("too late".into()),
            )
            .await;
        match refused {
            Err(crate::store::StoreError::Other(message)) => {
                assert!(
                    message.contains("already resolved"),
                    "the conflict is named: {message}"
                );
            }
            other => panic!("expected the conflict refusal, got {other:?}"),
        }

        let blocks = store.list_blocks(conv).await.unwrap();
        assert_eq!(blocks.len(), before, "the refused denial appends nothing");
        assert!(blocks.iter().all(|b| b.block_type != "approval_decision"));
        assert!(blocks.iter().all(|b| b.block_type != "tool_error"));
    }

    /// A denial targets its OWN call, keyed on the call block id: with two
    /// calls sharing one `tool_call_id`, the first call's result must not refuse
    /// the second call's denial — and after that denial, each call carries
    /// exactly one outcome and a second verdict against either is refused.
    #[tokio::test]
    async fn a_denial_targets_its_own_call_among_shared_tool_call_ids() {
        let (store, conv, first) = fixture().await;
        let second = store
            .insert_tool_call_block(
                conv,
                Role::Assistant,
                ToolCallInsert {
                    tool_call_id: "call_1".into(),
                    name: "danger".into(),
                    input: "{}".into(),
                    interactive: false,
                },
                None,
            )
            .await
            .unwrap();
        let first_request = store
            .insert_approval_request_block(conv, first)
            .await
            .unwrap()
            .expect("the first call's request writes");
        let second_request = store
            .insert_approval_request_block(conv, second)
            .await
            .unwrap()
            .expect("the second call's request writes");

        store
            .complete_tool_call_block(conv, "call_1".into(), "done".into(), first)
            .await
            .unwrap()
            .expect("the first call resolves");

        // The second call is unresolved: its denial must not be refused on the
        // strength of the FIRST call's result sharing the provider id.
        store
            .insert_approval_decision_block(
                conv,
                second_request,
                ApprovalChoice::Denied,
                None,
                Some("not this one".into()),
            )
            .await
            .expect("the sibling's result does not refuse this call's denial");

        let blocks = store.list_blocks(conv).await.unwrap();
        let errors: Vec<_> = blocks
            .iter()
            .filter(|b| b.block_type == "tool_error")
            .collect();
        assert_eq!(errors.len(), 1, "the denial resolved exactly one call");
        assert!(
            errors[0].fields["error"]
                .as_str()
                .unwrap()
                .contains("not this one")
        );

        // Both calls now carry an outcome; a denial of the first is refused.
        let refused = store
            .insert_approval_decision_block(
                conv,
                first_request,
                ApprovalChoice::Denied,
                None,
                Some("too late".into()),
            )
            .await;
        assert!(refused.is_err(), "the resolved first call takes no verdict");
    }

    /// The denial sentence has ONE construction site — the write itself. The
    /// store receives the reasons, never the rendered text, and the recorded
    /// sentence is exactly what [`denial_error_text`] builds from them.
    ///
    /// [`denial_error_text`]: crate::types::denial_error_text
    #[tokio::test]
    async fn the_denial_sentence_is_built_at_the_write() {
        let (store, conv, call) = fixture().await;
        let request = store
            .insert_approval_request_block(conv, call)
            .await
            .unwrap()
            .expect("the first request writes");
        store
            .insert_approval_decision_block(
                conv,
                request,
                ApprovalChoice::Denied,
                Some("policy".into()),
                Some("agreed".into()),
            )
            .await
            .unwrap();

        let blocks = store.list_blocks(conv).await.unwrap();
        let error = blocks
            .iter()
            .find(|b| b.block_type == "tool_error")
            .expect("the denial resolved the call");
        assert_eq!(
            error.fields["error"],
            Value::from(crate::types::denial_error_text(
                Some("policy"),
                Some("agreed")
            )),
            "the recorded sentence is the constructor's, verbatim"
        );
    }
}
