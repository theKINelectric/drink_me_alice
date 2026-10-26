# DrinkMeAlice

A small, bounded SurrealDB client for Elixir — **qualified against
SurrealDB 3.0.5** (see [Compatibility](#compatibility) for the exact
policy).

- **One supervised connection** (`DrinkMeAlice.Conn`) over WebSocket
  JSON-RPC: bounded admission, per-request deadlines, reconnect with
  backoff, optional live-table wakeup hints.
- **Honest writes** (`DrinkMeAlice.Tx`): every transaction ends as
  `committed`, `rejected`, `unknown` or `unavailable`. A lost reply is
  `unknown` — it may have committed — never reported as a failure.
- **Plain sessions** (`DrinkMeAlice.Session`) for scripts, setup and
  migrations; **provisioning** (`DrinkMeAlice.Admin`) of a least-privilege
  database user; a **locked, checksummed migration runner**
  (`DrinkMeAlice.Migrations`).
- **Graph edges** (`DrinkMeAlice.Graph`): `RELATE`, one-hop traversal.
- **An Ash data layer** (`DrinkMeAlice.Ash.DataLayer`), compiled only when
  your project uses Ash.

Everything that crosses the wire is bounded (message bytes, nesting,
keys, string sizes, elements), and failures that cross a library
boundary are typed values — including setup and validation errors that
some libraries let raise.

## Compatibility

The executable behavior this client encodes — transaction-reply
shapes, parameter coercion, protected parameter names — is **qualified
against SurrealDB 3.0.5 only**. Later 3.x builds are not automatically
that behavior: on an inspected 3.2.3, some aborted transactions return
a reply the classifier cannot qualify, and the client conservatively
reports `{:unknown, tx}` (never a false commit or a false definite
rejection) where 3.0.5 yields a definite rejection. That is the
intended conservative posture, not a bug to loosen.

Test runs enforce this: the live suite runs against 3.0.5 and **skips
with a printed reason** when the installed `surreal` (or `SURREAL_BIN`)
is any other version. Set `SURREAL_ALLOW_UNQUALIFIED=1` to run it
anyway as an explicit compatibility experiment — those results are not
a qualification. Each started test server records its full
`surreal version` identity as the first line of `server.log`.

## Deployment boundary: cleartext transport only

The only transport today is a cleartext WebSocket (`ws://`) over
HTTP/1: `Ws.connect/4` does not negotiate TLS, and no certificate or
hostname verification exists. Both runtime and root credentials travel
inside signin RPCs over that transport.

The supported deployment model is a **loopback or private-network
endpoint, or an externally protected tunnel** (SSH, WireGuard, a
service mesh). Using this client across an unprotected network is
unsupported — treat that as a hard boundary, not a configuration gap,
until a verified TLS/WSS configuration exists and is qualified.

## Install

```elixir
{:drink_me_alice, "~> 0.1"}
```

Developed and tested on Elixir 1.20 / OTP 29 (the JSON boundary uses
OTP 27+'s `:json`) against SurrealDB 3.0.5.

## Connect and write

```elixir
{:ok, _} =
  DrinkMeAlice.Conn.start_link(
    name: MyApp.Surreal,
    host: "127.0.0.1", port: 8000,
    ns: "app", db: "app",
    user: "app_user", pass: "…"          # database-scoped user by default
  )

{:ok, tx} =
  DrinkMeAlice.Tx.build(
    [
      "BEGIN TRANSACTION",
      "LET $q = (SELECT used, cap FROM ONLY quota:main)",
      "IF $q.used >= $q.cap { THROW \"quota_full\" }",
      "CREATE note SET body = #{DrinkMeAlice.exact_string_sql("body")}",
      "UPDATE quota:main SET used += 1",
      "COMMIT TRANSACTION",
      "RETURN {decision: \"created\"}"
    ],
    %{body: DrinkMeAlice.exact_string(body)}
  )

case DrinkMeAlice.Tx.run(MyApp.Surreal, tx, %{"created" => :committed}) do
  {:committed, _decision} -> :ok
  {:rejected, error} -> {:error, error}     # definitely not committed
  {:unknown, _tx} -> :reconcile             # may have committed: read before retrying
  {:unavailable, _error} -> :retry_later    # certified never sent
end
```

Reads: `DrinkMeAlice.query(MyApp.Surreal, "SELECT * FROM note WHERE …", params)`.

## Set up and migrate

```elixir
root = %{host: "127.0.0.1", port: 8000, ns: "app", db: "app", user: "root", pass: "…"}

:ok = DrinkMeAlice.Admin.provision(root, "app_user", app_pass)

:ok =
  DrinkMeAlice.Admin.migrate(root, [
    %{version: 1, name: "notes",
      statements: ["DEFINE TABLE note SCHEMAFULL", "DEFINE FIELD body ON note TYPE string"]}
  ])
```

Applied versions are skipped; an edited applied migration is refused
(checksum mismatch); a concurrent migrator is refused (lock).

## Ash

```elixir
defmodule MyApp.Crew.Mission do
  use Ash.Resource, domain: MyApp.Crew, data_layer: DrinkMeAlice.Ash.DataLayer

  surrealdb do
    conn MyApp.Surreal
    table "mission"
    max_rows 1_000
  end

  # attributes, identities, relationships, actions as usual
end

:ok = DrinkMeAlice.Admin.migrate(root, [
  DrinkMeAlice.Ash.Schema.migration(1, "crew", [MyApp.Crew.Mission])
])
```

Supported: create / read / update / destroy; filters `== != > >= < <= in
is_nil and or not` on attributes, with SQL three-valued logic; sort on
scalar attributes; limit/offset; `has_many` / `belongs_to` loading; unique
identities as indexes. Types: strings, UUIDs, integers, booleans, UTC and
naive datetimes, dates, times, decimals, floats, arrays of scalars, and
maps (stored as exact JSON text — not filterable). Reads are bounded by
`max_rows`: exceeding it is an error, never a silently truncated answer.
Ash needs `config :ash, default_string_length_count: :codepoints` (or
`:mixed`) in your app.

Not yet: cross-action transactions, upserts, bulk/atomic updates,
aggregates, data-layer calculations, multitenancy, filters across
relationships, edges as Ash relationships (use `DrinkMeAlice.Graph`).

## Know this about SurrealDB 3.0.5

JSON-RPC parameters that merely *look* typed are converted at any depth:
`"2026-10-01"` arrives as a datetime (and a `<string>` cast gives back
`"2026-10-01T00:00:00Z"`), `"task:abc"` as a record id, UUID-shaped
strings as uuids. Send exact strings with `DrinkMeAlice.exact_string/1`
and read them in SurrealQL with `DrinkMeAlice.exact_string_sql/1`.

Four parameter names are protected: `$session`, `$auth`, `$token` and
`$access`. Binding one makes the server refuse the whole request, so
`DrinkMeAlice` refuses them first with `{:error, {:protected_param, name}}`.

Also: SurrealQL is two-valued (`NONE <= 2` is true), JSON `null` is
rejected by `option<T>` fields (write `NONE`), and a failed transaction
reply is one entry shorter than the script.

## Tests

```sh
mix test                 # needs SurrealDB 3.0.5 (SURREAL_BIN or PATH)
mix test --exclude live  # no server needed
```

Live tests start a private loopback server per test module with data
under an exclusively claimed `System.tmp_dir!/0` directory, owned by a
dedicated process that stays alive through teardown; a teardown that
cannot be proven fails the run and retains the directory. A server of
any version other than 3.0.5 skips the live suite with the reason
printed (see [Compatibility](#compatibility)). The same helper is
public for projects built on this library:
`DrinkMeAlice.TestServer.start(dir: dir)` starts an owned SurrealDB
child (root credentials in `server.creds`), `stop/1` reaps it.

## License

MIT
