//! What "empty assistant content" means, for the endpoints that refuse it.
//!
//! A turn that completed without saying anything is recorded as an empty
//! assistant block, and the projection replays it. **That message is meant to
//! go out.** The model reading its own silence back is the point of recording
//! it, and the chat-completions schema agrees an empty string is a string. So
//! the default everywhere is to echo it, unchanged.
//!
//! Some endpoints refuse it anyway — anthropic will not take an empty text
//! block, and one gateway's own vendor wants content or tool calls. An
//! endpoint that refuses converts the message away **on its own side**, which
//! is why this module answers a question rather than enforcing a policy: it
//! says what counts as empty and records the removal, and each wire that needs
//! the answer asks for it. A wire whose endpoint accepts the echo never calls
//! in here at all.
//!
//! Empty means nothing survives once whitespace is trimmed — as a message, as a
//! text part, or as the whole of a parts list. A message still carrying a
//! non-text part keeps that part and loses only the empty text.
//!
//! The removal is a wire-side conversion and nothing more. None of these wires
//! merges adjacent messages: a merge would move a tool result away from the
//! message carrying the call it answers, which every endpoint here rejects.

use tracing::warn;

use super::types::{ContentPart, LlmError, MessageRole};

/// Whether the drop applies to this voice.
///
/// The scope is the assistant. An empty user or system group has a different
/// cause and is a different question; the wire that already guards those keeps
/// its own guards.
pub(crate) fn applies_to(role: MessageRole) -> bool {
    matches!(role, MessageRole::Assistant)
}

/// Whether a whole message's text survives, recording the message when it does
/// not.
///
/// The record is the point of returning through here rather than testing the
/// string at the call site: the debug dump a wire writes holds the messages
/// BEFORE conversion, so without a line in the log the logged request and the
/// sent request differ on exactly the path an operator would be reading.
pub(crate) fn keeps_message(role: MessageRole, text: &str) -> bool {
    if !is_empty(role, text) {
        return true;
    }
    warn!("an assistant message is empty once trimmed, so it is dropped");
    false
}

/// Whether one text contribution of a message survives, recording the
/// contribution when it does not.
///
/// A message that keeps a non-text part — media, a tool call, a tool result —
/// keeps the message and loses only this text. When nothing is left at all, the
/// wire's own "does this message carry anything" guard drops the message, and
/// these lines are the record of what went.
pub(crate) fn keeps_text_part(role: MessageRole, text: &str) -> bool {
    if !is_empty(role, text) {
        return true;
    }
    warn!("an assistant text part is empty once trimmed, so it is dropped");
    false
}

/// Whether a message's parts are worth converting at all, recording a group
/// that arrived carrying none.
///
/// A parts list with nothing in it is one of the shapes the render pass can
/// produce, and it is the one shape that has no text contribution to record a
/// drop for: every wire's own "does this message carry anything" guard would
/// discard it in silence, leaving the pre-conversion dump and the sent request
/// differing with nothing in between to explain it.
pub(crate) fn keeps_parts(role: MessageRole, parts: &[ContentPart]) -> bool {
    if !applies_to(role) || !parts.is_empty() {
        return true;
    }
    warn!("an assistant message carries no part at all, so it is dropped");
    false
}

/// Refuse a request that has no message to send.
///
/// Dropping never empties a request. The shape is degenerate — a conversation
/// whose only assistant block is empty rests its own frontier and dispatches
/// nothing — and the refusal still earns its line: a request with no message is
/// rejected by every endpoint here, with an error naming a position in an array
/// rather than the content that went missing.
///
/// # Errors
///
/// [`LlmError::NoMessage`] when the count is zero.
pub(crate) fn refuse_if_no_message(messages: usize) -> Result<(), LlmError> {
    if messages == 0 {
        return Err(LlmError::NoMessage);
    }
    Ok(())
}

/// Empty means: nothing survives once whitespace is trimmed.
///
/// An exact-empty test would still ship a whitespace-only message, which one of
/// these endpoints refuses just as firmly as the empty string.
fn is_empty(role: MessageRole, text: &str) -> bool {
    applies_to(role) && text.trim().is_empty()
}

/// Reading back what a drop recorded, for the pins that assert the record
/// rather than assume it.
///
/// It lives beside the decision rather than in one wire's test file because
/// every wire's drop is recorded through this module, and a wire pinning its
/// own line should not have to carry a collector of its own to do it. No
/// dependency is added for it.
#[cfg(test)]
pub(crate) mod capture {
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::{Event, Metadata, Subscriber, span};

    /// The lines one piece of work logged, in order.
    #[derive(Clone, Default)]
    struct Recorder(Arc<Mutex<Vec<String>>>);

    impl Recorder {
        fn lines(&self) -> Vec<String> {
            self.0.lock().expect("the recorder is not poisoned").clone()
        }
    }

    impl Subscriber for Recorder {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }

        fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

        fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut field = MessageField(String::new());
            event.record(&mut field);
            self.0
                .lock()
                .expect("the recorder is not poisoned")
                .push(format!("{} {}", event.metadata().level(), field.0));
        }

        fn enter(&self, _span: &span::Id) {}

        fn exit(&self, _span: &span::Id) {}
    }

    /// An event's own `message` field — the line a reader sees. Named for the
    /// field it reads, not for the layer's own word for a chat message.
    struct MessageField(String);

    impl Visit for MessageField {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = format!("{value:?}");
            }
        }
    }

    /// Run `work` with nothing but this collector listening, and hand back
    /// what it logged.
    pub(crate) fn recorded(work: impl FnOnce()) -> Vec<String> {
        let recorder = Recorder::default();
        tracing::subscriber::with_default(recorder.clone(), work);
        recorder.lines()
    }
}

#[cfg(test)]
mod tests {
    use super::capture::recorded;
    use super::*;

    /// Empty is a trim away, not an exact match, and only in the assistant's
    /// voice.
    #[test]
    fn empty_is_what_does_not_survive_a_trim() {
        assert!(!keeps_message(MessageRole::Assistant, ""));
        assert!(!keeps_message(MessageRole::Assistant, "   \n "));
        assert!(keeps_message(MessageRole::Assistant, " hi "));
        assert!(keeps_message(MessageRole::User, ""));
        assert!(keeps_message(MessageRole::System, "   "));
        assert!(!keeps_text_part(MessageRole::Assistant, "\t"));
        assert!(keeps_text_part(MessageRole::User, ""));
    }

    /// A group that arrived with no part at all is the one shape with no text
    /// contribution to speak for it, so it answers here — and only in the
    /// assistant's voice.
    #[test]
    fn a_parts_list_with_nothing_in_it_does_not_survive() {
        let some = [ContentPart::Text { text: "hi".into() }];
        assert!(!keeps_parts(MessageRole::Assistant, &[]));
        assert!(keeps_parts(MessageRole::Assistant, &some));
        assert!(keeps_parts(MessageRole::User, &[]));

        let lines = recorded(|| assert!(!keeps_parts(MessageRole::Assistant, &[])));
        assert_eq!(
            lines.len(),
            1,
            "the silent shape is the one that must speak"
        );
        assert!(
            lines[0].starts_with("WARN") && lines[0].contains("carries no part at all"),
            "the dropped message is recorded: {:?}",
            lines[0]
        );
    }

    /// Every drop is recorded, and nothing that survives is: an operator
    /// reconciling the pre-conversion dump against the sent request finds one
    /// line per thing that went, in the voice this tree uses for discarded
    /// content.
    #[test]
    fn each_drop_leaves_one_line_in_the_log() {
        let lines = recorded(|| {
            assert!(!keeps_message(MessageRole::Assistant, "  "));
            assert!(!keeps_text_part(MessageRole::Assistant, ""));
            assert!(keeps_message(MessageRole::Assistant, "kept"));
            assert!(keeps_text_part(MessageRole::User, ""));
        });

        assert_eq!(lines.len(), 2, "one line per drop and none besides");
        assert!(
            lines[0].starts_with("WARN") && lines[0].contains("assistant message is empty"),
            "the dropped message is recorded: {:?}",
            lines[0]
        );
        assert!(
            lines[1].starts_with("WARN") && lines[1].contains("assistant text part is empty"),
            "the dropped part is recorded: {:?}",
            lines[1]
        );
    }

    /// A request with nothing left to send is refused here, by name, rather
    /// than sent for an endpoint to refuse for a different-sounding reason.
    #[test]
    fn a_request_with_no_message_is_refused_by_name() {
        let err = refuse_if_no_message(0).expect_err("nothing to send is refused");
        assert!(matches!(err, LlmError::NoMessage));
        assert_eq!(err.to_string(), "the request has no message to send");
        assert!(!err.is_recoverable(), "re-sending it would fail the same");
        assert!(refuse_if_no_message(1).is_ok());
    }
}
