defmodule DrinkMeAlice.Tx do
  @moduledoc """
  Transaction scripts with a classifier-ready shape, and the one write
  path that maps transport facts plus the reply into a write outcome.

  A script is `BEGIN TRANSACTION; …; COMMIT TRANSACTION; RETURN {…}` —
  every decision executes inside the database transaction, and the
  trailing `RETURN` carries a map with a `decision` string. `build/2`
  records the positional context `%{commit: i, decision: j, len: n}`
  that `DrinkMeAlice.Outcome` needs: the pinned 3.0.5 executor pushes
  a COMMIT result only on success, so a failed reply is one entry
  shorter and a shifted `RETURN` must never read as commit success.

  String-typed params need care on 3.0.5, which coerces JSON params
  that merely look typed: a `<string>$p` cast repairs UUID-shaped
  strings and record-id-shaped strings, but NOT date-shaped strings
  (`"2026-10-01"` comes back rewritten even under the cast). For an
  exact string at any depth, send `DrinkMeAlice.exact_string/1` and
  read it back with `DrinkMeAlice.exact_string_sql/1`.
  """

  alias DrinkMeAlice.Error
  alias DrinkMeAlice.{Conn, Outcome}

  @max_statements 256
  # Aligned with the outbound encoder's per-string bound (Json.encode/1
  # caps every string at 65_536 bytes and the joined script is ONE
  # string parameter): a builder-approved script can never be refused
  # at encode time for size (the old 1 MiB builder bound was unreachable).
  @max_sql_bytes 65_536

  @enforce_keys [:sql, :vars, :ctx]
  defstruct [:sql, :vars, :ctx]

  @type t :: %__MODULE__{
          sql: binary(),
          vars: map(),
          ctx: %{commit: non_neg_integer(), decision: non_neg_integer(), len: pos_integer()}
        }

  @type verdicts :: %{
          binary() =>
            :committed | :existing | {:rejected, binary()} | {:rejected, binary(), term()}
        }

  @type outcome ::
          {:committed, map()}
          | {:existing, map()}
          | {:rejected, Error.t()}
          | {:unknown, t()}
          | {:unavailable, Error.t()}

  @doc """
  Assemble a script from its statements (without trailing `;`).

  The shape is checked, not assumed: the first statement is
  `BEGIN TRANSACTION`, exactly one `COMMIT TRANSACTION` sits immediately
  before the last statement, the last statement starts with `RETURN`,
  and the joined SQL is at most #{@max_sql_bytes} bytes.
  `{:error, {:bad_tx, why}}` otherwise.

  At least TWO in-transaction statements are required (5 statements
  total, at most #{@max_statements}). With a single in-tx statement the
  pinned executor's commit-failure rewrite is indistinguishable from a
  statement abort, so every failure would classify as `unknown` — a
  script that can never be definitely rejected. Prepend a harmless
  `LET` if the work is one statement.
  """
  @spec build([binary()], map()) :: {:ok, t()} | {:error, {:bad_tx, atom()}}
  def build(statements, vars \\ %{}) when is_list(statements) and is_map(vars) do
    len = length(statements)
    commits = Enum.count(statements, &(&1 == "COMMIT TRANSACTION"))

    cond do
      DrinkMeAlice.check_param_names(vars) != :ok ->
        DrinkMeAlice.check_param_names(vars)

      not Enum.all?(statements, &is_binary/1) ->
        {:error, {:bad_tx, :non_binary_statement}}

      len < 5 ->
        {:error, {:bad_tx, :too_few_statements}}

      len > @max_statements ->
        {:error, {:bad_tx, :too_many_statements}}

      hd(statements) != "BEGIN TRANSACTION" ->
        {:error, {:bad_tx, :no_begin}}

      commits != 1 ->
        {:error, {:bad_tx, :commit_count}}

      Enum.at(statements, len - 2) != "COMMIT TRANSACTION" ->
        {:error, {:bad_tx, :commit_position}}

      not String.starts_with?(List.last(statements), "RETURN") ->
        {:error, {:bad_tx, :no_return}}

      true ->
        assemble(statements, vars, len)
    end
  end

  defp assemble(statements, vars, len) do
    sql = Enum.join(statements, ";\n") <> ";"

    if byte_size(sql) > @max_sql_bytes do
      {:error, {:bad_tx, :sql_too_large}}
    else
      {:ok,
       %__MODULE__{
         sql: sql,
         vars: DrinkMeAlice.stringify_param_names(vars),
         ctx: %{commit: len - 2, decision: len - 1, len: len}
       }}
    end
  end

  @doc """
  Submit `tx` through a `Conn` owner and classify the result.

  - `{:committed, decision_map}` / `{:existing, decision_map}` — per
    `verdicts`, with the whole `RETURN` map;
  - `{:rejected, error}` — a mapped rejection verdict or a qualified
    statement abort (definitely not committed);
  - `{:unknown, tx}` — the request may have committed (lost reply,
    inconclusive reply shape). Reconcile by resubmitting the identical
    idempotent script or by reading; never treat as failure;
  - `{:unavailable, error}` — certified not submitted (credit bound,
    connection down, deadline passed before send, session refused,
    latched clock uncertainty — gated at dispatch inside the owner), or
    the request was over the size bound.

  The whole submission — admission, deadline and the clock gate — runs
  under `Conn.call/3`'s bounded protocol; there is no separate control
  call, so a missing owner is a typed `{:unavailable, _}` and a blocked
  owner is bounded by the caller's own deadline.

  Options: `deadline_ms`, `class`.
  """
  @spec run(GenServer.server(), t(), verdicts(), keyword()) :: outcome()
  def run(conn, %__MODULE__{} = tx, verdicts, opts \\ []) when is_map(verdicts) do
    request = %{method: "query", params: [tx.sql, tx.vars], kind: :mutation}

    case Conn.call(conn, request, Keyword.take(opts, [:deadline_ms, :class])) do
      {:reply, decoded} -> classify(decoded, verdicts, tx)
      {:lost, _reason} -> {:unknown, tx}
      {:refused, reason} -> refused(reason)
    end
  end

  @doc "Classify an already-received reply for `tx` (exposed for callers with their own transport)."
  @spec classify(map(), verdicts(), t()) :: outcome()
  def classify(decoded, verdicts, %__MODULE__{} = tx) do
    case Outcome.classify_tx(decoded, verdicts, tx.ctx) do
      {:committed, d} -> {:committed, d}
      {:existing, d} -> {:existing, d}
      {:rejected, %Error{} = e} -> {:rejected, e}
      {:session_refused} -> {:unavailable, Error.new("database_unavailable", :session_refused)}
      {:refused, _code} -> {:unavailable, Error.new("database_unavailable", :refused)}
      {:inconclusive, _why} -> {:unknown, tx}
    end
  end

  # Every certified non-send reason maps explicitly — including
  # `:too_large` (the owner refuses oversized requests before reserving
  # a credit) and `:clock_uncertain` (the owner's dispatch-time gate for
  # mutations).
  defp refused(:exhausted), do: {:unavailable, Error.new("rpc_budget_exhausted", :credit_bound)}

  defp refused(reason) when reason in [:down, :expired],
    do: {:unavailable, Error.new("database_unavailable", reason)}

  defp refused(:clock_uncertain),
    do: {:unavailable, Error.new("database_unavailable", :clock_uncertain)}

  defp refused(:too_large), do: {:unavailable, Error.bounded_exceeded(:request_bytes)}
  defp refused(:encode), do: {:unavailable, Error.new("invalid_input", :request_encode)}
  defp refused(other), do: {:unavailable, Error.new("database_unavailable", other)}
end
