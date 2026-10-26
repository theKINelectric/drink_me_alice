defmodule DrinkMeAlice do
  @moduledoc """
  A bounded SurrealDB client for Elixir, qualified against SurrealDB
  3.0.5 (see the README's Compatibility section for the version policy
  and the cleartext-transport deployment boundary).

  Layers, bottom up:

  - `DrinkMeAlice.Ws` / `FrameGuard` / `Json` — Mint WebSocket
    transport with frame bounds and a bounded JSON decode profile.
  - `DrinkMeAlice.Conn` — one supervised connection owner with
    credit-bounded admission, deadlines and honest transport facts.
  - `DrinkMeAlice.Session` — a caller-owned, synchronous socket for
    short-lived work (scripts, provisioning, migrations).
  - `DrinkMeAlice.Outcome` + `DrinkMeAlice.Tx` — transaction scripts
    and the reply classifier that never collapses `unknown` into
    `rejected` or `committed`.
  - `DrinkMeAlice.Admin` + `DrinkMeAlice.Migrations` — root-only
    provisioning and a checksummed, locked migration runner.
  - `DrinkMeAlice.Ash.DataLayer` — an Ash data layer (compiled only
    when the consumer brings Ash).

  This module holds the read-side helper `query/4` and the shared
  identifier check.
  """

  alias DrinkMeAlice.Conn

  @max_ident_bytes 64

  @doc """
  True for a SurrealQL identifier this connector will splice into SQL:
  `[A-Za-z_][A-Za-z0-9_]*`, at most #{@max_ident_bytes} bytes. Everything
  else travels as a bound `$param`.
  """
  @spec identifier?(term()) :: boolean()
  def identifier?(s) when is_binary(s),
    do: byte_size(s) in 1..@max_ident_bytes and s =~ ~r/\A[A-Za-z_][A-Za-z0-9_]*\z/

  def identifier?(_), do: false

  @doc """
  Wire value for an EXACT string parameter — pair it with `exact_string_sql/1`.

  SurrealDB 3.0.5's JSON-RPC parameter conversion coerces strings that
  merely look typed — at any depth, inside arrays and objects too:

  | sent               | arrives as          | `<string>` of it            |
  |--------------------|---------------------|-----------------------------|
  | `"2026-10-01"`     | `datetime`          | `"2026-10-01T00:00:00Z"` ✗  |
  | `"task:abc"`       | record id           | `"task:abc"`                |
  | UUID-shaped string | `uuid`              | the same string             |

  A `<string>$p` cast repairs the UUID
  and record-id cases but NOT date-shaped strings, which come back
  rewritten. Prefixing one character that no coercion recognises and
  slicing it off server-side is exact for every string (verified against
  the pinned server, including the empty string and non-ASCII text).
  """
  @spec exact_string(binary()) :: binary()
  def exact_string(value) when is_binary(value), do: "~" <> value

  @doc "The SurrealQL expression that recovers a `exact_string/1` param named `name`."
  @spec exact_string_sql(binary()) :: binary()
  def exact_string_sql(name), do: "string::slice($#{name}, 1)"

  @doc "The SurrealQL expression that recovers a list of `exact_string/1` values in param `name`."
  @spec exact_string_list_sql(binary()) :: binary()
  def exact_string_list_sql(name), do: "$#{name}.map(|$s| string::slice($s, 1))"

  @protected ~w(access auth session token)

  @doc """
  SurrealDB 3.0.5 protects the parameter names `$#{Enum.join(@protected, "`, `$")}`:
  binding one makes the server refuse the WHOLE request (JSON-RPC -32600,
  measured). `:ok`, or `{:error, {:protected_param, name}}` before sending.
  Only atom and binary key names are inspected; other key types are
  rejected later by the outbound encoder with its own typed error, so
  this check never raises on them.
  """
  @spec check_param_names(map()) :: :ok | {:error, {:protected_param, binary()}}
  def check_param_names(vars) when is_map(vars) do
    case Enum.find(Map.keys(vars), &(param_key?(&1) and to_string(&1) in @protected)) do
      nil -> :ok
      name -> {:error, {:protected_param, to_string(name)}}
    end
  end

  defp param_key?(k) when is_atom(k) or is_binary(k), do: true
  defp param_key?(_), do: false

  @doc """
  Normalize a query's parameter map: atom names become strings (the
  outbound encoder (`DrinkMeAlice.Json.encode/1`) accepts only
  string keys). Values must already be canonical-domain terms — binaries,
  i64 integers, booleans, `nil`, lists and string-keyed maps; floats,
  decimals and datetimes travel as strings with a SurrealQL cast.
  """
  @spec stringify_param_names(map()) :: map()
  def stringify_param_names(vars) when is_map(vars) do
    Map.new(vars, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} -> {k, v}
    end)
  end

  @doc """
  Run one read-side `query` through a `Conn` owner.

  Returns `{:ok, results}` where `results` is the per-statement array
  (`%{"status" => "OK" | "ERR", "result" => _}` each — callers check
  every status), or `{:error, reason}`:

  - `{:rpc_error, code}` / `:session_refused` — the server refused;
  - `{:lost, reason}` — sent, no reply (for a write this is an UNKNOWN
    outcome, never a failure — use `DrinkMeAlice.Tx.run/4` for writes);
  - `{:refused, reason}` — certified never sent (`:down`, `:expired`,
    `:exhausted`, `:too_large`, `:encode`).

  Options: `deadline_ms` (≤ 2_000), `class` (`:general` | `:internal`).
  """
  @spec query(GenServer.server(), binary(), map(), keyword()) ::
          {:ok, list()} | {:error, term()}
  def query(conn, sql, vars \\ %{}, opts \\ []) when is_binary(sql) and is_map(vars) do
    with :ok <- check_param_names(vars), do: do_query(conn, sql, vars, opts)
  end

  defp do_query(conn, sql, vars, opts) do
    case Conn.call(
           conn,
           %{method: "query", params: [sql, stringify_param_names(vars)], kind: :read},
           opts
         ) do
      {:reply, %{"result" => results}} when is_list(results) ->
        {:ok, results}

      {:reply, %{"error" => %{} = err}} ->
        if DrinkMeAlice.Outcome.session_refusal?(err),
          do: {:error, :session_refused},
          else: {:error, {:rpc_error, Map.get(err, "code")}}

      {:reply, _other} ->
        {:error, :bad_reply}

      {:lost, reason} ->
        {:error, {:lost, reason}}

      {:refused, reason} ->
        {:error, {:refused, reason}}
    end
  end
end
