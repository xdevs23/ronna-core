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
