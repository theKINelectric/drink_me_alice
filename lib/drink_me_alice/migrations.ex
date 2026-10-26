defmodule DrinkMeAlice.Migrations do
  @moduledoc """
  A checksummed, locked, versioned migration runner; the migration list
  is supplied by the caller.

  Bookkeeping: `schema_migration` rows record `started`/`applied`/`failed` per version with the SHA-256
  of the version's joined statements; a `migration_lock:main` row makes
  concurrent ownership a hard conflict. The runner refuses a database
  that holds a lock it did not take, a `started`-but-unfinished row, or a
  checksum that disagrees with its source.

  ## Evidence rules (shared with `DrinkMeAlice.Outcome`)

  Every transaction-shaped operation — lock acquisition, migration
  application — is classified by `DrinkMeAlice.Outcome.classify_tx/3`
  with the script's positional context, so the runner uses the SAME
  three-way distinction as the write path:

  - **observed commit** — full-length reply, well-formed entries, OK
    `BEGIN`/`COMMIT` slots with nil results, mapped decision. Only this
    is success;
  - **qualified abort** — the pinned executor's statement-abort
    silhouette (exactly one real error, `NotExecuted` markers before it,
    `Cancelled` markers after). Only this is a definite failure, and
    only for it may the `failed` bookkeeping mark be written;
  - **uncertain** — the commit-failure rewrite (multiple rewritten
    `Internal` errors; the storage commit may already be durable),
    malformed or unexpected-length replies. The runner writes NOTHING
    and retains the lock: `{:error, {:lock_retained,
    {:migration_unknown, version, why}}}`. Reconcile by reading
    `schema_migration` for `version` (an `applied` row with the expected
    checksum means the migration committed) and then removing the lock
    row; anything else needs investigation before a retry.

  Non-transaction operations (bootstrap, the state read, the release
  DELETE) demand their own complete evidence: exact statement count,
  well-formed `{status, result}` entries on every slot, and the expected
  result shape. An empty or malformed reply is a refusal, never proof of
  execution. A release that cannot be proven is reported
  (`{:lock_release_unproven, _}`), never discarded, and is never
  attempted through a session that already reported transport loss.

  Callbacks (`vars`, `verify_applied`) and the caller's `query` function
  may crash without leaking the lock: a callback crash becomes a typed
  error with the lock released; a query-function crash during an
  operation with durable effect is treated exactly like transport loss.
  """

  alias DrinkMeAlice.Outcome

  @type query :: (binary(), map() -> {:ok, list()} | {:error, term()})

  @type migration :: %{
          required(:version) => pos_integer(),
          required(:name) => binary(),
          required(:statements) => [binary()],
          optional(:seed) => [binary()],
          optional(:vars) => (keyword() -> {:ok, map()} | {:error, term()}),
          optional(:verify_applied) => (query() -> :ok | {:error, term()})
        }

  @max_migrations 256
  @max_statements 1_024
  @lock_tag "bf:migration_locked"

  @bootstrap [
    "DEFINE TABLE IF NOT EXISTS schema_migration SCHEMAFULL",
    "DEFINE FIELD IF NOT EXISTS version ON TABLE schema_migration TYPE int",
    "DEFINE FIELD IF NOT EXISTS checksum ON TABLE schema_migration TYPE string",
    "DEFINE FIELD IF NOT EXISTS state ON TABLE schema_migration TYPE string",
    "DEFINE FIELD IF NOT EXISTS owner ON TABLE schema_migration TYPE string",
    "DEFINE FIELD IF NOT EXISTS started_at ON TABLE schema_migration TYPE datetime",
    "DEFINE FIELD IF NOT EXISTS finished_at ON TABLE schema_migration TYPE option<datetime>",
    "DEFINE FIELD IF NOT EXISTS detail ON TABLE schema_migration TYPE option<string>",
    "DEFINE INDEX IF NOT EXISTS migration_version_uq ON TABLE schema_migration FIELDS version UNIQUE",
    "DEFINE TABLE IF NOT EXISTS migration_lock SCHEMAFULL",
    "DEFINE FIELD IF NOT EXISTS owner ON TABLE migration_lock TYPE string",
    "DEFINE FIELD IF NOT EXISTS acquired_at ON TABLE migration_lock TYPE datetime"
  ]

  @doc "SHA-256 (lowercase hex) of a statement list, joined by newline."
  @spec checksum([binary()]) :: binary()
  def checksum(statements) do
    :crypto.hash(:sha256, Enum.join(statements, "\n")) |> Base.encode16(case: :lower)
  end

  @doc """
  Validate a migration list: contiguous versions from 1, names that are
  identifiers, non-empty binary statement lists within bounds. Every
  member must be a map — anything else is a typed refusal, never a
  raise.
  """
  @spec check([migration()]) :: :ok | {:error, term()}
  def check(migrations) when is_list(migrations) do
    cond do
      migrations == [] ->
        {:error, {:invalid_migrations, :empty}}

      not Enum.all?(migrations, &is_map/1) ->
        {:error, {:invalid_migrations, :not_a_map}}

      length(migrations) > @max_migrations ->
        {:error, {:invalid_migrations, :too_many}}

      Enum.map(migrations, &Map.get(&1, :version)) != Enum.to_list(1..length(migrations)) ->
        {:error, {:invalid_migrations, :versions_not_contiguous}}

      true ->
        Enum.find_value(migrations, :ok, fn m ->
          stmts = Map.get(m, :statements)
          seed = Map.get(m, :seed, [])

          cond do
            not DrinkMeAlice.identifier?(Map.get(m, :name)) ->
              {:error, {:invalid_migrations, {:name, Map.get(m, :version)}}}

            not (is_list(stmts) and stmts != [] and Enum.all?(stmts, &is_binary/1)) ->
              {:error, {:invalid_migrations, {:statements, Map.get(m, :version)}}}

            not (is_list(seed) and Enum.all?(seed, &is_binary/1)) ->
              {:error, {:invalid_migrations, {:seed, Map.get(m, :version)}}}

            length(stmts) + length(seed) > @max_statements ->
              {:error, {:invalid_migrations, {:too_many_statements, Map.get(m, :version)}}}

            true ->
              nil
          end
        end)
    end
  end

  @doc """
  Apply every pending migration over an authenticated root `query`
  (see `DrinkMeAlice.Admin.migrate/3`). `owner` is a fresh per-run id.

  - `:ok` — every migration observed committed (or already applied with
    matching checksums) AND the lock release was proven;
  - `{:error, :locked}` — another owner holds `migration_lock`;
  - `{:error, {:dirty, version, detail}}` — a `started`/`failed` row or a
    checksum mismatch; refuses until reconciled by hand;
  - `{:error, {:lock_state_unknown, detail}}` — lock acquisition ended
    uncertain (transport loss, a crashed query function, or an
    unqualified reply): the lock may or may not be held. Inspect
    `migration_lock` before retrying;
  - `{:error, {:lock_retained, {:migration_unknown, version, detail}}}` —
    a migration transaction's outcome is UNCERTAIN (commit-failure
    rewrite, malformed/unexpected reply, crashed query function).
    Nothing was overwritten and the lock is still held; reconcile as the
    module doc describes before retrying;
  - `{:error, {:lock_release_unproven, detail}}` — the run itself is
    known (`{:run_ok, reason}` or `{:run_error, run_reason, reason}`) but
    the release DELETE could not be proven; the lock row may remain;
  - `{:error, {:callback_crash, class}}` — a `vars`/`verify_applied`
    callback crashed before anything was submitted for that migration;
    the lock WAS released;
  - `{:error, {:incomplete_reply, got, expected}}` — bootstrap evidence
    was empty or truncated (bootstrap runs before any lock exists);
  - `{:error, {:bad_statement, detail}}` — a malformed bootstrap entry;
  - `{:error, {:statement_err, msg}}` — a QUALIFIED statement abort (per
    `Outcome`) in a transaction, or a definite statement error outside
    one;
  - `{:error, {:invalid_migrations, _}}` — the list failed `check/1`;
  - `{:error, reason}` — transport/statement failure or a
    `verify_applied`/`vars` refusal.
  """
  @spec apply_all(query(), binary(), [migration()], keyword()) :: :ok | {:error, term()}
  def apply_all(query, owner, migrations, opts \\ []) do
    with :ok <- check(migrations),
         :ok <- bootstrap(query),
         :acquired <- acquire_lock(query, owner) do
      finish(query, owner, run_pending(query, owner, migrations, opts))
    end
  end

  # The lock is ours from acquisition on: every exit path must either
  # PROVE the release or report exactly how it is unproven/retained —
  # never a silent `:ok` or a guessed failure with the lock row present.
  defp finish(_query, _owner, {:error, {:migration_unknown, _, _} = unknown}) do
    {:error, {:lock_retained, unknown}}
  end

  defp finish(_query, _owner, {:error, {:lost, _} = lost}) do
    # A query function that reported transport loss is not reusable for
    # the release, and the operation may or may not have committed.
    {:error, {:lock_release_unproven, {:run_error, lost, :transport_lost}}}
  end

  defp finish(query, owner, run) do
    case release_lock(query, owner) do
      :ok ->
        case run do
          :ok -> :ok
          {:error, r} -> {:error, r}
        end

      {:error, rr} ->
        case run do
          :ok -> {:error, {:lock_release_unproven, {:run_ok, rr}}}
          {:error, r} -> {:error, {:lock_release_unproven, {:run_error, r, rr}}}
        end
    end
  end

  # Bootstrap is not a transaction: complete evidence is the exact
  # per-statement array of well-formed entries. A definite statement
  # error here is a real error (no rewrite shapes exist outside
  # transactions); an empty, truncated or malformed array is a refusal.
  defp bootstrap(query) do
    case safe_query(query, Enum.join(@bootstrap, ";\n") <> ";", %{}) do
      {:ok, results} -> expect_bootstrap(results)
      {:crashed, class} -> {:error, {:query_crashed, class}}
      {:error, r} -> {:error, r}
    end
  end

  defp expect_bootstrap(results) when is_list(results) do
    got = length(results)

    cond do
      got != length(@bootstrap) ->
        {:error, {:incomplete_reply, got, length(@bootstrap)}}

      not Enum.all?(results, &entry_shape?/1) ->
        {:error, {:bad_statement, :malformed_entry}}

      true ->
        Enum.find_value(results, :ok, fn
          %{"status" => "OK"} -> nil
          %{"status" => "ERR", "result" => msg} -> {:error, {:statement_err, prefix(msg)}}
        end)
    end
  end

  defp expect_bootstrap(_), do: {:error, {:bad_statement, :not_a_list}}

  # `<string>$owner` casts defeat 3.0.5's JSON-param uuid coercion, which
  # would otherwise fail the `owner TYPE string` field.
  defp acquire_lock(query, owner) do
    sql = """
    BEGIN TRANSACTION;
    LET $held = (SELECT owner FROM ONLY migration_lock:main);
    IF $held != NONE AND $held.owner != <string>$owner {
      THROW "#{@lock_tag}";
    };
    UPSERT ONLY migration_lock:main SET owner = <string>$owner, acquired_at = time::now();
    COMMIT TRANSACTION;
    RETURN {decision: "acquired"};
    """

    case safe_query(query, sql, %{"owner" => owner}) do
      {:crashed, class} ->
        {:error, {:lock_state_unknown, {:query_crashed, class}}}

      {:error, {:lost, r}} ->
        {:error, {:lock_state_unknown, r}}

      {:error, r} ->
        {:error, r}

      {:ok, results} ->
        case classify_tx(results, %{"acquired" => :committed}, 6) do
          {:committed, _decision} ->
            :acquired

          {:rejected, %{detail: {:tx_aborted, msg}}} ->
            # A qualified abort of the acquisition transaction: the lock
            # was definitely NOT acquired — no release is owed.
            if String.contains?(msg, @lock_tag),
              do: {:error, :locked},
              else: {:error, {:statement_err, prefix(msg)}}

          {:rejected, other} ->
            {:error, {:lock_state_unknown, {:unqualified_rejection, other}}}

          {:inconclusive, why} ->
            # Commit-failure rewrite or malformed reply: the UPSERT may
            # be durable — say exactly that.
            {:error, {:lock_state_unknown, why}}
        end
    end
  end

  defp release_lock(query, owner) do
    case safe_query(query, "DELETE FROM migration_lock:main WHERE owner = <string>$owner;", %{
           "owner" => owner
         }) do
      {:ok, results} when is_list(results) ->
        cond do
          length(results) != 1 ->
            {:error, {:incomplete_reply, length(results), 1}}

          match?(%{"status" => "OK", "result" => rows} when is_list(rows), hd(results)) ->
            :ok

          match?(%{"status" => "ERR", "result" => msg} when is_binary(msg), hd(results)) ->
            {:error, {:statement_err, prefix(hd(results)["result"])}}

          true ->
            {:error, {:release_shape, :malformed}}
        end

      {:ok, _other} ->
        {:error, {:release_shape, :not_a_list}}

      {:crashed, class} ->
        {:error, {:release_crashed, class}}

      {:error, r} ->
        {:error, r}
    end
  end

  defp run_pending(query, owner, migrations, opts) do
    Enum.reduce_while(migrations, :ok, fn m, :ok ->
      m = Map.put(m, :checksum, checksum(m.statements))

      case migration_state(query, m) do
        {:ok, :applied} ->
          case safe_step(fn -> verify_applied(query, m) end) do
            :ok -> {:cont, :ok}
            {:error, r} -> {:halt, {:error, r}}
          end

        {:ok, :absent} ->
          case apply_one(query, owner, m, opts) do
            :ok -> {:cont, :ok}
            {:error, r} -> {:halt, {:error, r}}
          end

        {:ok, {:dirty, detail}} ->
          {:halt, {:error, {:dirty, m.version, detail}}}

        {:error, r} ->
          {:halt, {:error, r}}
      end
    end)
  end

  # A crashed caller callback (vars/verify_applied). These run before
  # (or after, read-only) any submission for the step in question, so
  # the failure is definite for that migration — finalization still
  # executes and releases the lock (R-02).
  defp safe_step(fun) do
    fun.()
  rescue
    e -> {:error, {:callback_crash, crash_class(e)}}
  catch
    kind, _value -> {:error, {:callback_crash, {:caught, kind}}}
  end

  defp verify_applied(query, %{verify_applied: f}) when is_function(f, 1), do: f.(query)
  defp verify_applied(_query, _m), do: :ok

  defp migration_state(query, m) do
    sql = "SELECT version, checksum, state FROM schema_migration WHERE version = $v LIMIT 1;"

    case safe_query(query, sql, %{"v" => m.version}) do
      {:ok, results} when is_list(results) ->
        cond do
          length(results) != 1 ->
            {:error, {:incomplete_reply, length(results), 1}}

          match?(%{"status" => "OK", "result" => rows} when is_list(rows), hd(results)) ->
            decode_state(hd(results)["result"], m)

          match?(%{"status" => "ERR", "result" => msg} when is_binary(msg), hd(results)) ->
            {:error, {:statement_err, prefix(hd(results)["result"])}}

          true ->
            {:error, {:bad_statement, :malformed_state_reply}}
        end

      {:ok, _other} ->
        {:error, {:bad_statement, :not_a_list}}

      {:crashed, class} ->
        {:error, {:query_crashed, class}}

      {:error, r} ->
        {:error, r}
    end
  end

  defp decode_state(rows, m) do
    case rows do
      [] -> {:ok, :absent}
      [%{"state" => "applied", "checksum" => c} | _] when c == m.checksum -> {:ok, :applied}
      [%{"state" => "applied", "checksum" => c} | _] -> {:ok, {:dirty, {:checksum_mismatch, c}}}
      [%{"state" => s} | _] -> {:ok, {:dirty, {:state, s}}}
      _other -> {:ok, :absent}
    end
  end

  defp apply_one(query, owner, m, opts) do
    with {:ok, seed_vars} <- safe_step(fn -> seed_vars(m, opts) end) do
      statements =
        [
          "BEGIN TRANSACTION",
          "CREATE schema_migration CONTENT {version: $v, checksum: <string>$cs, state: \"started\", owner: <string>$owner, started_at: time::now(), finished_at: NONE, detail: NONE}"
        ] ++
          m.statements ++
          Map.get(m, :seed, []) ++
          [
            "UPDATE schema_migration SET state = \"applied\", finished_at = time::now() WHERE version = $v",
            "COMMIT TRANSACTION",
            "RETURN {decision: \"applied\", version: $v}"
          ]

      vars = Map.merge(seed_vars, %{"v" => m.version, "cs" => m.checksum, "owner" => owner})

      case safe_query(query, Enum.join(statements, ";\n") <> ";", vars) do
        {:crashed, class} ->
          # The caller's query function crashed mid-operation: the
          # submission state is unknown — exactly the uncertain case.
          {:error, {:migration_unknown, m.version, {:query_crashed, class}}}

        {:error, {:lost, r}} ->
          {:error, {:lost, r}}

        {:error, r} ->
          {:error, r}

        {:ok, results} ->
          case classify_tx(results, %{"applied" => :committed}, length(statements)) do
            {:committed, _decision} ->
              :ok

            {:rejected, %{detail: {:tx_aborted, msg}}} ->
              # QUALIFIED abort — a definite non-commit certificate. The
              # whole transaction (including its `started` mark) rolled
              # back, so the best-effort failure mark cannot overwrite
              # an applied marker: there is no row to overwrite.
              mark_failed(query, m)
              {:error, {:statement_err, prefix(msg)}}

            {:rejected, other} ->
              {:error, {:migration_unknown, m.version, {:unqualified_rejection, other}}}

            {:inconclusive, why} ->
              # UNCERTAIN (commit-failure rewrite, malformed entries,
              # unexpected count): write NOTHING, keep the lock — the
              # reconciliation procedure in the module doc decides.
              {:error, {:migration_unknown, m.version, why}}
          end
      end
    end
  end

  # Best-effort failure mark — only ever called with a definite
  # non-commit certificate in hand (R-01). Its own failure leaves the
  # row absent, which stays dirty and safe.
  defp mark_failed(query, m) do
    _ =
      safe_query(
        query,
        "UPDATE schema_migration SET state = \"failed\", detail = <string>$d, finished_at = time::now() WHERE version = $v;",
        %{"v" => m.version, "d" => "statement_error"}
      )

    :ok
  end

  defp seed_vars(%{vars: f}, opts) when is_function(f, 1) do
    case f.(opts) do
      {:ok, vars} when is_map(vars) -> {:ok, DrinkMeAlice.stringify_param_names(vars)}
      {:error, r} -> {:error, r}
      other -> {:error, {:bad_seed_vars, other}}
    end
  end

  defp seed_vars(_m, _opts), do: {:ok, %{}}

  # -- statement-result plumbing ------------------------------------------------

  # Call the caller's query function, converting a crash into a typed
  # fact instead of an exception (R-02). A crashed call is NOT a
  # certified non-send — the function may have submitted before raising
  # — so callers treat `{:crashed, _}` like transport loss wherever the
  # operation has durable effect.
  defp safe_query(query, sql, vars) do
    query.(sql, vars)
  rescue
    e -> {:crashed, crash_class(e)}
  catch
    kind, _value -> {:crashed, {:caught, kind}}
  end

  # The exception module only — never the message, which can carry
  # caller data. (Rescued values are always exception structs.)
  defp crash_class(%{__struct__: mod}), do: mod

  # Classify one of this runner's transaction scripts through the SAME
  # evidence rules as the write path: observed commit, qualified abort,
  # or uncertain — never a guessed failure (R-01).
  defp classify_tx(results, verdicts, statement_count) do
    Outcome.classify_tx(
      %{"result" => results},
      verdicts,
      %{
        commit: statement_count - 2,
        decision: statement_count - 1,
        len: statement_count
      }
    )
  end

  # Every statement entry must carry both `status` and `result` — a
  # `{status: OK}` entry without a result slot is malformed evidence,
  # not success (R-03).
  defp entry_shape?(%{"status" => s, "result" => _}) when s in ["OK", "ERR"], do: true
  defp entry_shape?(_), do: false

  defp prefix(s) when is_binary(s), do: String.slice(s, 0, 80)
  defp prefix(o), do: inspect(o) |> String.slice(0, 80)
end
