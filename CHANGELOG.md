# Changelog

## 0.1.0 (unreleased)

First release. Extracted from the Bifrost framework's SurrealDB adapter
(history and per-file provenance in `PROVENANCE.md`), made framework-free:
supervised connection, sessions, transactions with honest outcomes,
provisioning, migrations, graph edges, Ash data layer.

### TestServer data-dir exclusivity (2026-09-27, sibling flake; corrected after validation V-01..V-04)

- Concurrent `TestServer.start/1` calls on the SAME data dir used to race
  into SurrealDB's SurrealKV LOCK: the loser died with
  `{:startup_failed, {:early_exit, 1}, ...}` and its log read
  "Database at .../data/LOCK is already locked by another process" —
  reproduced with two concurrent starts on one dir. A second feeder,
  verified by SIGKILLing the owning BEAM: the orphaned child survives,
  keeps the LOCK, and the next run onto that store flaked the same way.
  What is now PROVEN to be refused before spawning:
  - within the VM, each start claims its expanded data dir through a
    DEDICATED holder process (one caller may hold several stores at
    once; the holder monitors the port owner, so a claim never
    outlives its owner), and the second caller on the same dir fails
    with `{:error, {:data_dir_in_use, dir, holder_pid}}` before any
    filesystem effect;
  - against an existing store, a bounded best-effort `ps` scan refuses
    with `{:error, {:data_dir_lock_suspected, dir, suspects}}` when a
    process whose executable is named `surreal` carries the exact
    `surrealkv://dir/data` endpoint as a whitespace-delimited argv
    token — verified against a real foreign `surreal start` child and
    the killed-VM orphan. This is SUSPICION, not verified lock
    ownership: a command-line match is a hint to identify and confirm
    the process before stopping it; do not kill on a match alone, and
    never delete a store that may still be active;
  - claims are released ONLY by their own start's holder and ONLY on
    proven cleanup (`{:exit_status, _}` or reaped-without-status): a
    stale handle stopped after a replacement start cannot strip the
    replacement's claim, and an unproven cleanup leaves the exclusion
    in place until the owning process dies. `stop/1`-released claims
    keep the documented restart-on-same-dir flow working;
  - an early exit that still slips through the scan's race window is
    reported with the log tail and the suspected foreign holders.
- NOT claimed: preventing orphans (a spawned child can outlive its
  owning process and a SIGKILLed VM — measured on this runtime), and
  race-free cross-VM exclusion (the scan is fail-open and start-moment
  only). Directories are claimed by expanded path; symlink aliases are
  not resolved.
- Follow-up corrections (validation W-01..W-04): claim release is now
  a BOUNDED SYNCHRONOUS handshake — the holder removes the
  registration and acknowledges before `stop/1` (or a pre-spawn failed
  start) reports completion, so an immediate same-dir restart is never
  refused by our own just-released claim; a holder that cannot answer
  (e.g. adversarially suspended) bounds the wait with a diagnostic
  instead of hanging. A failed start now carries the claim identity
  through acquisition: pre-spawn failures release synchronously,
  startup failures are finalized by `stop/1` under the same
  identity+proof gate, and UNPROVEN failed-startup cleanup retains the
  claim — the error carries the holder pid as a receipt, and the
  exclusion clears when the starting process dies. The lifecycle
  regressions install teardown at acquisition (failure-safe
  try/after boundaries, child exit proven before any store deletion),
  and the polling helpers carry one absolute deadline instead of
  renewing it per retry.
- Gap closure (validation round 2, W-01..W-03 at the timeout and
  cleanup-failure boundaries): when the bounded release handshake
  times out, the PUBLIC result preserves both facts as
  `{:error, {:claim_release_unconfirmed, holder, cleanup}}` — for
  `stop/1`, through startup finalization, and for pre-spawn failed
  starts — instead of hiding a pending release behind a successful
  child result; the suspended-holder regression now requires that
  result. Acquisition state is represented directly: every failure
  after the port exists is wrapped `{:acquisition_unclean, reason,
  holder}` at the site where that state is known (covering the rescued
  `port_info` branch, not just `:no_os_pid`) and retains the claim
  with the receipt. Test teardown returns explicit evidence —
  `teardown_foreign_holder/4` reports `{:error, {:exit_unobserved,
  pid}}` instead of a silent `:ok`, store deletion happens only on
  observed child exit (uncertainty fails the test and retains the
  directory), a forced-unconfirmed-teardown control exercises that
  path, and the deadline regression's closed port comes from a child
  whose exit was observed, not from closing a port on a live sleep.
- Round 3 (validation probes): diagnostics are BEST-EFFORT — a group
  leader that answers IO requests with an error cannot prevent startup
  finalization (previously the raise escaped the health rescue
  handler, the outer rescue released the claim, and the child kept
  running; runtime-reproduced with a controlled qualified-version
  executable that execs sleep). A hard finalization boundary now wraps
  everything from acquisition: any exception after the child exists —
  including one raised inside a rescue handler — becomes
  `{:startup_failed, {:finalization_crashed, cause}, ...}` with proven
  cleanup or a retained claim with the holder receipt, making the
  outer rescue pre-spawn-only by construction. Test-side, directory
  deletion authority is generation-scoped: once the first server is
  proven stopped and the replacement attempt begins, first-generation
  cleanup never deletes the shared directory — the replacement's
  retention decision governs (regression: deterministic non-owner
  replacement stop forces cleanup uncertainty and verifies the store
  survives every enclosing cleanup block, claim still held; the
  replacement fixture itself is failure-safe — its shutdown boundary
  is installed when the owner is created, covering startup failure and
  timeouts, tears down in proof order with retention-and-receipt on
  unproven cleanup, and an intentional body-failure control verifies
  the full shutdown).

### Repairs from the 2026-09-26 Astra audit

- **Session setup (A-01, widened by review R-06):** `Session.open/1`
  normalizes omitted `ns`/`db` to explicit `nil`s before connecting, so
  a root config with the keys omitted behaves exactly like `ns: nil,
  db: nil` — previously it raised a `KeyError` whose message printed the
  credential-bearing config map and leaked the already-connected socket.
  The ENTIRE setup boundary — normalization, endpoint/timeout
  validation (integer port in range, non-negative integer
  `connect_timeout`), connect and establishment — runs under
  rescue/catch that closes any acquired socket and returns a sanitized
  typed error; `open/1` neither raises nor exits on any configuration
  shape, including malformed top-level forms.
- **Write path (A-02):** the mutation clock gate moved from a
  pre-admission `GenServer.call` in `Tx.run/4` (which exited the caller
  with `:noproc` on a missing owner and blocked a full 5 s regardless of
  the caller's deadline) into the connection owner's dispatch, serial
  with the owner's work, immediately before the frame is written, under
  the credit/deadline protocol. Uncertainty latched after admission now
  still refuses the mutation before send (regression-tested: no frame
  reaches the peer). A missing owner is a typed `{:unavailable, _}`;
  a suspended owner is bounded by the caller's deadline.
- **Hint subscriptions (A-04):** `Conn.hint_subscribe/2` is idempotent —
  exactly one monitor per subscriber PID. A repeat subscription used to
  stack a fresh monitor behind the 16-PID set bound, and an existing
  subscriber could receive `:full` at capacity; both fixed and tested.
- **Migrations (A-06, corrected by review R-01/R-02/R-03):** every
  transaction-shaped operation (lock acquisition, migration application)
  is classified by `DrinkMeAlice.Outcome` with its script's positional
  context — the same three-way evidence rules as the write path.
  Observed commit (full-length, OK/nil BEGIN and COMMIT slots, mapped
  decision) is the only success; a QUALIFIED abort silhouette is the
  only definite failure (and only for it may the `failed` mark be
  written); everything else — including the commit-failure rewrite
  (multiple rewritten `Internal` errors, where the storage commit may
  already be durable) — is UNCERTAIN: nothing is overwritten, the lock
  is retained, and the result says so
  (`{:error, {:lock_retained, {:migration_unknown, version, why}}}`)
  with a documented reconciliation procedure. Non-transaction operations
  (bootstrap, state read, release) demand exact counts, well-formed
  `{status, result}` entries on every slot, and the expected result
  shape — an empty, truncated or malformed array is never proof of
  execution. Lock acquisition ending uncertain (transport loss or a
  crashed query function) reports `{:lock_state_unknown, _}`; an
  unproven release is surfaced as `{:lock_release_unproven, _}` and
  never attempted through a session that reported transport loss.
  Crashing `vars`/`verify_applied` callbacks finalize normally
  (`{:callback_crash, class}`, lock released); a crashed query function
  during an operation with durable effect is treated like transport
  loss. `check/1` refuses non-map members with a typed error instead of
  raising.
- **Version qualification (A-03):** the package claims SurrealDB 3.0.5,
  not "3.x". `TestServer` resolves the executable, records its full
  `surreal version` identity as the first line of `server.log`, and
  refuses to start a different version unless
  `SURREAL_ALLOW_UNQUALIFIED=1` explicitly opts into a compatibility
  experiment. Live tests skip with a printed reason on any other
  installed version. Known 3.2.3 behavior under that opt-in: aborted
  transactions return reply shapes the 3.0.5-qualified classifier cannot
  qualify, and the client conservatively reports unknown — never a false
  commit or a false definite rejection. After the review correction
  pass this family grew from four write-path cases to include the
  migration runner (a 3.2.3 abort arrives as a full-length reply with a
  pre-commit error, not the qualified 3.0.5 abort silhouette, so
  `Admin.migrate` there reports `{:lock_state_unknown, _}` /
  `{:migration_unknown, _}` instead of a definite result; successful
  migrations still apply and verify normally) — preserved by design
  until 3.2.3 is qualified explicitly.
- **Live fixture (A-07):** the test server's port is owned by a
  dedicated owner process that stays alive through teardown (the
  `setup_all` process is gone before `on_exit` runs — that is why exit
  statuses were unreadable and TERM grace burned on every module).
  Connections are stopped before the server; the data directory is
  claimed exclusively with a collision-resistant name and removed only
  when cleanup is proven; an unproven teardown fails the run and
  retains the directory, with a receipt for late readers. The live
  suite's teardown time dropped from ~35 s to ~5 s.
- **Transport boundary (A-05, documented):** the README states the
  deployment boundary prominently — cleartext WebSocket only, loopback /
  private network / protected tunnel; unprotected networks are
  unsupported until verified TLS/WSS exists.
- **Transport regressions (A-08, partial):** dedicated local
  FrameGuard/Ws boundary suite (declared-length attacks before body
  bytes, fragmented aggregates at and over the bound, malformed control
  frames, ping floods, close echo — each rejection paired with a nearby
  valid control and a transport-release assertion). CI added for the
  qualified combination (Elixir 1.20 / OTP 29 / SurrealDB 3.0.5) plus a
  no-Ash consumer build check (`scripts/check-no-ash-consumer.sh`).

### Review correction pass (2026-09-27)

- **R-01:** migration uncertainty is no longer converted into a definite
  failure — see the corrected Migrations entry above. The shared
  SURREAL-004 lesson was corrected alongside.
- **R-04:** a late `Live.stop!/1` on an already-exited fixture owner
  (monitor sees `:noproc`) now resolves through the teardown receipt
  instead of failing, and receipts are deleted only after a proven
  outcome — unproven cleanup keeps its evidence.
- **R-05:** the qualified CI job runs the explicit downloaded
  `./surreal`, verifies the release tarball against a committed
  SHA-256, and sets `SURREAL_REQUIRE_QUALIFIED=1` — a strict mode in
  which anything that would skip the live suite (wrong version,
  unusable binary) FAILS the run instead. `TestServer.resolve_bin/0` is
  bounded (2 s probe) and typed; a failed or hung version command is an
  error, never a parsed substring.
- Test quality: the clock-gate race regression now holds the trigger
  reply behind an explicit barrier and observes the mutation
  admitted-and-enqueued via the credit table itself; transport burst
  tests synchronize on the verdict rather than loopback packetization;
  the no-Ash consumer check executes a public-API smoke demo instead of
  compiling only.

### Smaller repairs

- `Tx.build/2`'s SQL bound is aligned with the encoder's per-string cap
  (65_536 bytes): the previous 1 MiB builder bound was unreachable — a
  script over 64 KiB was always refused at encode time.
- `Tx` module docs no longer recommend `<string>$p` for all strings —
  date-shaped strings survive only via `DrinkMeAlice.exact_string/1`.
- `Json` module introduction describes the decoded-byte preflight the
  implementation actually performs (the old text described a raw-span
  scan).
- `DrinkMeAlice.check_param_names/1` no longer raises on non-atom,
  non-binary map keys; they are left to the encoder's typed rejection.
- `TestServer`'s un-reaped-child diagnostics use `/proc` where present
  and fall back to `ps` elsewhere (no more Linux-shaped placeholders on
  macOS).

### Release preparation still open

- Package `links` are empty pending a public repository home.
