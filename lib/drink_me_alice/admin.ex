defmodule DrinkMeAlice.Admin do
  @moduledoc """
  Short-lived administrative work under the root identity: provisioning
  (namespace, database, the database-scoped runtime user) and schema
  migrations. Never pooled, never a process — each call opens its own
  `DrinkMeAlice.Session`, runs, and closes.

  The runtime identity is separate: `provision/4` defines a
  database-scoped `EDITOR` user and nothing here keeps admin material.
  Credentials never enter logs or error values.

  `config` is a `Session` config whose `user`/`pass` are the ROOT
  credentials (`signin: :root` is the default).

  Every value spliced
  into DDL is validated first: names must be identifiers
  (`DrinkMeAlice.identifier?/1`), durations must match `<n><unit>`, and the
  runtime password must be 1–128 bytes of `[A-Za-z0-9_.~-]` — so no
  quote or statement separator can reach the SQL text.
  """

  alias DrinkMeAlice.{Migrations, Session}

  @duration ~r/\A[1-9][0-9]{0,5}(ms|s|m|h|d|w)\z/
  @password ~r/\A[A-Za-z0-9_.~-]{1,128}\z/

  @doc """
  Define the namespace/database if absent and the database-scoped runtime
  EDITOR user. Options: `token_dur` (default `"1h"`), `session_dur`
  (default `"8h"`).
  """
  @spec provision(map() | keyword(), binary(), binary(), keyword()) :: :ok | {:error, term()}
  def provision(config, runtime_user, runtime_pass, opts \\ []) do
    define_user(config, runtime_user, runtime_pass, opts, "IF NOT EXISTS", true)
  end

  @doc "Force-overwrite the runtime user's credentials (rotation / recovery)."
  @spec rotate_runtime_user(map() | keyword(), binary(), binary(), keyword()) ::
          :ok | {:error, term()}
  def rotate_runtime_user(config, runtime_user, runtime_pass, opts \\ []) do
    define_user(config, runtime_user, runtime_pass, opts, "OVERWRITE", false)
  end

  defp define_user(config, user, pass, opts, mode, with_ns_db?) do
    c = Map.new(config)
    token_dur = Keyword.get(opts, :token_dur, "1h")
    session_dur = Keyword.get(opts, :session_dur, "8h")

    with :ok <- check(:ns, c[:ns], &DrinkMeAlice.identifier?/1),
         :ok <- check(:db, c[:db], &DrinkMeAlice.identifier?/1),
         :ok <- check(:runtime_user, user, &DrinkMeAlice.identifier?/1),
         :ok <- check(:runtime_pass, pass, &(is_binary(&1) and &1 =~ @password)),
         :ok <- check(:token_dur, token_dur, &(is_binary(&1) and &1 =~ @duration)),
         :ok <- check(:session_dur, session_dur, &(is_binary(&1) and &1 =~ @duration)) do
      # On 3.0.5 an `ON NAMESPACE` clause on DEFINE DATABASE is rejected
      # (generic Internal -32000); `use` has already selected the
      # namespace, so the bare form is correct.
      user_stmt =
        "DEFINE USER #{mode} #{user} ON DATABASE PASSWORD '#{pass}' " <>
          "ROLES EDITOR DURATION FOR TOKEN #{token_dur}, FOR SESSION #{session_dur}"

      stmts =
        if with_ns_db?,
          do: [
            "DEFINE NAMESPACE IF NOT EXISTS #{c.ns}",
            "DEFINE DATABASE IF NOT EXISTS #{c.db}",
            user_stmt
          ],
          else: [user_stmt]

      Session.with_session(c, &run_statements(&1, stmts))
    end
  end

  defp check(key, value, pred) do
    if pred.(value), do: :ok, else: {:error, {:invalid_option, key}}
  end

  @doc """
  Apply `migrations` (see `DrinkMeAlice.Migrations`) through one root
  session. Options: `owner` (default a fresh UUIDv4), `reply_budget_ms`
  (5_000..60_000, default 5_000 — widen for migrations whose audit
  statements scan large tables; a reply wait, not a cancellation),
  plus anything the migrations' own `vars` functions read.
  """
  @spec migrate(map() | keyword(), [Migrations.migration()], keyword()) :: :ok | {:error, term()}
  def migrate(config, migrations, opts \\ []) do
    owner = Keyword.get(opts, :owner, uuid4())

    with {:ok, budget} <- reply_budget(opts),
         :ok <- Migrations.check(migrations) do
      config
      |> Map.new()
      |> Map.put(:reply_budget_ms, budget)
      |> Session.with_session(&Migrations.apply_all(&1, owner, migrations, opts))
    end
  end

  defp reply_budget(opts) do
    case Keyword.get(opts, :reply_budget_ms, 5_000) do
      v when is_integer(v) and v in 5_000..60_000 -> {:ok, v}
      other -> {:error, {:invalid_option, :reply_budget_ms, other}}
    end
  end

  defp run_statements(query, stmts) do
    case query.(Enum.join(stmts, ";\n") <> ";", %{}) do
      {:ok, results} ->
        Enum.find_value(results, :ok, fn
          %{"status" => "OK"} -> nil
          %{"status" => "ERR", "result" => m} -> {:error, {:statement_err, prefix(m)}}
          _ -> {:error, :bad_statement}
        end)

      {:error, r} ->
        {:error, r}
    end
  end

  defp prefix(s) when is_binary(s), do: String.slice(s, 0, 80)
  defp prefix(o), do: inspect(o) |> String.slice(0, 80)

  @doc false
  def uuid4 do
    <<a::32, b::16, c::16, d::16, e::48>> = :crypto.strong_rand_bytes(16)
    c = Bitwise.bor(Bitwise.band(c, 0x0FFF), 0x4000)
    d = Bitwise.bor(Bitwise.band(d, 0x3FFF), 0x8000)

    :io_lib.format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [a, b, c, d, e])
    |> IO.iodata_to_binary()
  end
end
