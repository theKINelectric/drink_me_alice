# Provenance

DrinkMeAlice began as `bifrost_surreal` (this repository's history up to
the rename), which was extracted from Bifrost as recorded below.

The code was extracted on 2026-09-24 from the Bifrost umbrella app
`bifrost_memory` in the `fly_bridge` checkout, branch `flybridge/mvp`,
commit `2722ad69044ce15c87606b49b2416f1676162d9e`
(`elixir/apps/bifrost_memory/lib/bifrost/memory/`).

The source adapter's qualification evidence (BF-02 receipts and manifests)
covers the **source files at that commit**, not this library. Nothing
here inherits that acceptance; this library's evidence is its own test
suite, run against the same pinned SurrealDB 3.0.5 artifact.

## Source files (SHA-256 at the source commit)

| Source | SHA-256 | Here | Change |
|---|---|---|---|
| `ws.ex` | `773d36a2f3ace80cb87417fb195656ee26b03e396b996e3b2d48b9a67aee6db0` | `lib/drink_me_alice/ws.ex` | module rename only |
| `frame_guard.ex` | `616806ab2ff03e2034b9ba9d381b8c5970ed41d209d2586276f3e7875fb10a5a` | `frame_guard.ex` | module rename only |
| `json.ex` | `56325ef50af5774a9856e633f077ba07035a19ec4312f40ec6acec0262fda7bb` | `json.ex` | decoder unchanged; own encoder (see below) |
| `credits.ex` | `449482c623125b8ca5299a47754d5192a04f93e092ca0e98e3ebc7b0602b926f` | `credits.ex` | module rename only |
| `outcome.ex` | `defcba648d028c5b18b5867c50e15899414dff09e6a9eaf4c0378a7e15e30cf5` | `outcome.ex` | rename; returns `DrinkMeAlice.Error` |
| `conn.ex` | `ac719479b86ef61b57a9feadbdf4da48eddba36f09347178bf736e3e86527b59` | `conn.ex` | generalized — see below |
| `admin.ex` | `ee4a17f304a92063289c1d794db231fb2da5362655c4906ef4ddd08e3de5ea56` | `admin.ex`, `session.ex` | split and hardened — see below |
| `migrations.ex` | `9873d36bfa547d764aa1b8d70ae1a4d783bafc616e0b1df4ff36172cb85afab4` | `migrations.ex` | runner kept, Bifrost schema removed |
| `sql.ex` | `b096e3cc2bc77510bd5fd0f307faf87f37c769b4efd5b222f081e7b67fe1797d` | `tx.ex` | only `assemble/2` → `Tx.build/2` |

Tests ported from `elixir/apps/bifrost_memory/test/` at the same commit:
`json_test`, `outcome_test`, `credits_test` (rename only) and `conn_test`
(rename, the generalized config, and the two review-44 regressions now
drive `Tx.run/4` with the same 13-statement wire shapes instead of
`Bifrost.Memory.accept/2`). Support: `fake_surreal`, `synth_peer`,
`test_server` (rename only).

## What changed, and why

**No framework dependency.** The source used `Bifrost.Contracts.Error`
and `Bifrost.Contracts.Codec.encode/2`. A leaf library that depends on
the framework using it makes a loop (Mix refuses to unify the umbrella's
copy with a path dependency on the same app), so the library owns a
two-field `DrinkMeAlice.Error` (same codes) and a bounded canonical
encoder over OTP `:json` (same term rules, the decode profile's bounds;
`nil` is written as JSON `null` — OTP alone would write `"nil"`).
Bifrost maps the error onto its own in one place
(`Bifrost.Memory.classify_tx/3`).

**`Conn`** — the workflow coupling became configuration: the live table
(`live: "work_item"` was hard-coded), the clock-floor query
(`Bifrost.Memory.Sql.clock_floor_query/0` → optional `clock_query`), the
hint message tag, and the signin scope (`:database` default, as before;
`:namespace`/`:root` added). The config is validated before a process
starts. Clock latching now only runs when a clock query is configured, so
application rows carrying a `regressed` key cannot latch it.

**`Session`** (new, from `Admin`'s session internals) — a caller-owned,
synchronous socket for scripts, provisioning and migrations. Root sessions
may omit `ns`/`db`.

**`Admin`** — `provision/4` and `rotate_runtime_user/4` validated nothing
they spliced into DDL; the runtime password was interpolated into
`PASSWORD '…'`. Every spliced value is now validated (identifiers,
durations, a password charset without quotes). `define_async_hook/3`
(work_item-specific) was not carried over.

**`Migrations`** — the runner (bootstrap, lock, checksums, dirty
detection, one transaction per version) is unchanged in behaviour and
bookkeeping format; the migration list, seeds and applied-integrity checks
are supplied by the caller. Lists are validated before a socket opens.

**`Tx`** — `Bifrost.Memory.Sql.assemble/2` plus the transport-fact mapping
from `Bifrost.Memory.do_mutation/4`. Two fixes over the source:

1. `{:refused, :too_large}` from `Conn.call/3` had no clause in
   `Bifrost.Memory.refused/2` (a `FunctionClauseError` for an oversized
   request); it is now `{:unavailable, bounded_limit_exceeded}`.
2. `build/2` refuses scripts with fewer than two in-transaction
   statements, which the classifier can never judge as rejected.

## Findings against the pinned server

Recorded by probes and pinned in `test/drink_me_alice/live_test.exs` and
`ash_data_layer_test.exs`:

- JSON-RPC parameter conversion coerces date-shaped strings
  (`"2026-10-01"` → datetime), record-shaped strings (`"task:abc"`) and
  UUID-shaped strings, at any depth. The source adapter's `<string>$p`
  convention repairs UUIDs and record ids but **not** dates:
  `<string>` of `"2026-10-01"` is `"2026-10-01T00:00:00Z"`. Exact strings
  need `DrinkMeAlice.exact_string/1` + `exact_string_sql/1`.
- `type::thing` no longer exists in 3.0.5 (use `type::record`).
- JSON `null` is rejected by `option<T>` fields; write `NONE`.
- `NONE <= 2` is true and `!(NONE = 1)` is true: SurrealQL is two-valued,
  so the Ash filter translation implements SQL three-valued logic itself.
