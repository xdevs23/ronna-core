//! The isolation rule, checked over this crate's own source.
//!
//! The goldens in each vendor module pin what that vendor's wire looks like.
//! This checks the other half of the rule — that nothing *else* builds one —
//! which no golden can, because a golden only sees the file it lives in.
//!
//! It reads the source tree rather than the compiled crate on purpose. The
//! failure being guarded against is a wire field name appearing somewhere it
//! does not belong, and a field name is a string: it survives into no type a
//! test could reflect over, and it is exactly what a well-meaning edit adds to
//! shared code when a vendor needs "just one more field".

use std::path::{Path, PathBuf};

/// Markers that belong to exactly one vendor's request or response body.
///
/// Each is a field name a vendor's API defines. A file outside that vendor's
/// own module has no business naming one: if it does, shared code has learned a
/// wire shape.
///
/// Two families are deliberately absent. The names the *stored* continuity
/// payload carries are one — that payload is per-vendor by design, is persisted
/// beside the block it belongs to, and is therefore named by the ledger itself,
/// which is the whole reason it is opaque rather than interpreted. Generic
/// words are the other: a marker that also names a module or a column produces
/// hits that teach a reader to ignore this test, which is worse than not having
/// it.
const WIRE_MARKERS: &[&str] = &[
    "anthropic-version",
    "completion_tokens_details",
    "content_block_delta",
    "finish_reason",
    "function_call_output",
    "input_schema",
    "max_output_tokens",
    "output_tokens_details",
    "prompt_cache_key",
    "reasoning_content",
    "reasoning_effort",
    "signature_delta",
    "stream_options",
    "summary_index",
    "supported_efforts",
    "thinking_delta",
];

/// Files allowed to name a wire marker: the vendor modules, and this file.
fn is_vendor_source(path: &Path) -> bool {
    let text = path.to_string_lossy().replace('\\', "/");
    [
        "/providers/anthropic",
        "/providers/chat",
        "/providers/kimi",
        "/providers/mistral",
        "/providers/openai",
        "/providers/openrouter",
        "/providers/isolation_tests.rs",
    ]
    .iter()
    .any(|allowed| text.contains(allowed))
}

/// Whole-word, case-insensitive containment, matching how the vocabulary check
/// is specified.
///
/// A plain substring match would flag the shorter terms inside unrelated
/// identifiers and make the list unusable. The check has to be precise enough
/// that a hit is always a real one, because a check that cries wolf is a check
/// people learn to skip.
fn contains_word(haystack: &str, needle: &str) -> bool {
    let boundary = |c: char| !c.is_alphanumeric() && c != '_';
    let mut from = 0;
    while let Some(at) = haystack[from..].find(needle) {
        let start = from + at;
        let end = start + needle.len();
        let before_ok = haystack[..start].chars().next_back().is_none_or(boundary);
        let after_ok = haystack[end..].chars().next().is_none_or(boundary);
        if before_ok && after_ok {
            return true;
        }
        from = start + 1;
    }
    false
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("the source tree is readable") {
        let path = entry.expect("a readable directory entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// No file outside a vendor module names a vendor's wire.
///
/// This is the check that the neutral layer really is neutral. A failure here
/// names the file and the marker, because "somewhere in the crate" is not
/// actionable and the person reading it is looking at a red suite, not at this
/// comment.
#[test]
fn no_wire_shape_is_built_outside_a_vendor_module() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_sources(&src, &mut files);
    assert!(
        files.len() > 20,
        "the source scan found {} files, which means it is not looking where it thinks",
        files.len()
    );

    let mut leaks = Vec::new();
    for path in files.iter().filter(|p| !is_vendor_source(p)) {
        let body = std::fs::read_to_string(path).expect("a readable source file");
        for marker in WIRE_MARKERS {
            if body.contains(marker) {
                leaks.push(format!("{} names `{marker}`", path.display()));
            }
        }
    }

    assert!(
        leaks.is_empty(),
        "a wire shape escaped its vendor module:\n{}",
        leaks.join("\n")
    );
}

/// Every HTTP client in this crate is built through the one guarded
/// constructor.
///
/// The guard lives in that constructor, so it covers a client built through it
/// and nothing else — which makes "every client comes from there" the whole of
/// what the guard guarantees, and an unenforced claim about it worthless. One
/// test had already built its own client directly, against a comment saying
/// none could.
///
/// The scan is textual for the same reason the wire scan is: the bypass is a
/// call that compiles perfectly well, and no type a test could reflect over
/// records which constructor a client came from.
#[test]
fn no_file_outside_the_guarded_constructor_builds_a_client() {
    // Every reqwest construction form: the plain constructor, the default,
    // and both spellings of the builder. A scan that knew only one form let a
    // builder-built client through while the prose claimed none could exist.
    const BYPASS: [&str; 4] = [
        "Client::new",
        "Client::default",
        "Client::builder",
        "ClientBuilder::new",
    ];

    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_sources(&src, &mut files);

    let mut hits = Vec::new();
    for path in &files {
        let name = path.to_string_lossy().replace('\\', "/");
        // The constructor itself, and this file, which holds the marker.
        if name.ends_with("/providers/http.rs") || name.ends_with("/providers/isolation_tests.rs") {
            continue;
        }
        let body = std::fs::read_to_string(path).expect("a readable source file");
        if BYPASS.iter().any(|form| body.contains(form)) {
            hits.push(format!("{} builds a client of its own", path.display()));
        }
    }

    assert!(
        hits.is_empty(),
        "a client escaped the guarded constructor:\n{}",
        hits.join("\n")
    );
}

/// The vocabulary this library must not speak, checked where it would appear.
///
/// The list is committed beside the extraction spec so the check has a referent
/// instead of being an aspiration. A term joins it when a product concept is
/// found leaking, which is why the list is data and this test is three lines of
/// logic.
#[test]
fn no_source_file_speaks_a_product_vocabulary() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the crate sits two levels below the repository root");
    let list = std::fs::read_to_string(repo.join("docs/forbidden-vocabulary.txt"))
        .expect("the vocabulary list is committed with the spec");
    let terms: Vec<&str> = list
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();
    assert!(terms.len() > 10, "the vocabulary list did not parse");

    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_sources(&src, &mut files);

    // This file is checked like every other, and holds no term of its own: the
    // list arrives by reading it. An exemption here would be the one place a
    // leak could hide from its own checker.
    let mut hits = Vec::new();
    for path in &files {
        let body = std::fs::read_to_string(path)
            .expect("a readable source file")
            .to_lowercase();
        for term in &terms {
            if contains_word(&body, &term.to_lowercase()) {
                hits.push(format!("{} names `{term}`", path.display()));
            }
        }
    }

    assert!(
        hits.is_empty(),
        "a product concept leaked into the library:\n{}",
        hits.join("\n")
    );
}
