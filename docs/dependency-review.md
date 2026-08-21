# Dependency review

Every dependency is checked before it is added, and the check is recorded here. Two questions,
both answered against a source rather than from memory:

1. **What is the current version?** Looked up on the registry at the time of adding, never
   recalled. A version chosen from memory is a version chosen at random.
2. **Does that exact version carry a known compromise?** Checked against the advisory database.
   A version number alone is not evidence of safety — malicious releases are published to
   package registries regularly, and a fresh version is exactly what one looks like.

A dependency that fails either question does not go in. One that passes is recorded below with
the date it was checked, because "we checked" is only useful if the reader can see when.

## 2026-08-20 — the foundation set

Resolved versions taken from the lockfile, current versions from the registry API, advisories
from the OSV aggregate (which carries the Rust advisory database).

| Crate | Resolved | Latest at check | Advisories | Why it is here |
|---|---|---|---|---|
| serde | 1.0.229 | 1.0.229 | none | Serialization of the ledger's value types |
| serde_json | 1.0.151 | 1.0.151 | none | The block payload is a JSON object by nature |
| tokio | 1.53.1 | 1.53.1 | none | The async runtime the reactive primitives and the bus need |
| tracing | 0.1.44 | 0.1.44 | none | Structured logging |
| strum | 0.28.0 | 0.28.0 | none | Deriving the stable string label an event carries |

All five resolved to the current latest, so nothing here is pinned behind a newer release.

**The test-only split.** The async runtime appears twice on purpose: the feature set the
library needs is small, and the extra features the tests need — a macro attribute, a
multi-threaded runtime flavour, a timer — sit in the development section instead. Cargo unifies
features across a dependency graph, so a feature enabled for a test would otherwise be
compiled into every project that depends on this one.

## 2026-08-21 — the store

Same two questions, same sources: resolved versions from the lockfile, current versions from
the registry API, advisories from the OSV aggregate.

| Crate | Resolved | Latest at check | Advisories | Why it is here |
|---|---|---|---|---|
| rusqlite | 0.40.2 | 0.40.2 | none | The embedded database the ledger is stored in |
| libsqlite3-sys | 0.38.2 | 0.38.2 | none | Pulled in by `rusqlite`; the engine itself, compiled from vendored source |
| chrono | 0.4.45 | 0.4.45 | none | Local-timezone dates, which the date marker records and no standard-library type produces |
| thiserror | 2.0.20 | 2.0.20 | none | The store's error type |

All four resolved to the current latest.

**The two features asked of the database crate.** `bundled` compiles the engine from vendored
source rather than linking whatever the host happens to have: a consumer needs no system
library, and every machine gets the same engine, which matters for a library whose tests assert
on schema behaviour. `hooks` is the row change hook — the store's whole connection to the
scheduler's heartbeat is that callback, so it is not optional here.

**The one transitive dependency named.** `libsqlite3-sys` is not declared in any manifest; it
is recorded because it is where the C engine actually comes from, and a review that checked the
wrapper but not the thing it wraps would be checking the wrong artifact.

## 2026-08-21 — the provider layer

The model boundary is the first part of this library that speaks to the outside world, so it is
the first that needs an HTTP client, a server-sent-events reader and stream combinators. Same
two questions, same sources: current versions from the registry API, advisories from the OSV
aggregate. The resolved column is what the lockfile settled on after these were declared.

| Crate | Resolved | Latest at check | Advisories | Why it is here |
|---|---|---|---|---|
| reqwest | 0.13.4 | 0.13.4 | none | The HTTP client every vendor module streams over |
| futures | 0.3.34 | 0.3.34 | none | The stream contract itself — an event stream is a `Stream` |
| eventsource-stream | 0.2.3 | 0.2.3 | none | Server-sent events, the wire every vendor's stream arrives on |
| tokio-util | 0.7.19 | 0.7.19 | none | The cancellation token a turn is interrupted through |
| uuid | 1.24.1 | 1.24.1 | none | One vendor's client identifier, behind that vendor's feature |

All five resolved to the current latest.

**Which of these are optional.** `uuid` and `eventsource-stream` arrive by feature only: `uuid`
with the one vendor that needs a client identifier, `eventsource-stream` with any vendor at all,
since nothing but a vendor module reads a wire. The other three are unconditional because the
always-present layer names them — the stream type is a `futures::Stream`, the error type
classifies a `reqwest::Error` by whether it carries an HTTP status, and a turn is cancelled
through a `tokio_util` token. Gating them would make the error vocabulary depend on which
vendors a consumer compiled, which is the one thing a uniform stream contract cannot afford.

**The TLS question, answered by not answering it.** This manifest names no TLS feature. The HTTP
crate's own default is rustls with a vendored provider, so a consumer needs no system library and
every build gets the same stack — the reasoning that put `bundled` on the database crate. Taking
the default is what keeps this library out of a consumer's TLS decision.

**The transitive dependencies named.** None of these is declared in any manifest; they are
recorded because they are where the bytes actually go, and a review that checked the client but
not the stack under it would be checking the wrong artifact.

| Crate | Resolved | Advisories | What it is |
|---|---|---|---|
| hyper | 1.11.0 | none | The HTTP implementation the client is a facade over |
| rustls | 0.23.43 | none | The TLS implementation, arriving with the client's default |
| aws-lc-sys | 0.44.0 | none | The C cryptography that backs it, compiled from vendored source |

**No test here reaches a vendor.** Every HTTP client is built through one constructor, and in
test builds that constructor binds the client's socket to the loopback interface, so the
operating system refuses a connection to any other address and an outbound request fails at
connect. A client built anywhere else would not carry the guard, so a source scan fails the suite
on any file outside that constructor that builds one. What the mechanism guarantees is therefore
exactly this: nothing a test sends can leave the machine — a connection to this machine's own
loopback still can be made, and the tests that assert the guard rely on that. It is a property of
the code rather than of the runner, which is why it is recorded beside the dependency that would
otherwise make it possible.
