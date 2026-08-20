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
