defmodule DrinkMeAlice.MigrationsTest do
  @moduledoc """
  Migration-runner evidence regressions (A-06 + review R-01/R-02/R-03),
  all offline over a scriptable `query` function — the boundary
  `apply_all/4` actually trusts.

  The scripted peer replies with the REAL 3.0.5 wire shapes, including
  the transaction silhouettes `Outcome` qualifies: full-length observed
  commits (OK/nil BEGIN and COMMIT slots, mapped decision), qualified
  statement aborts (one real error, `NotExecuted` markers before,
  `Cancelled` markers after, reply one entry short), and the
  commit-failure rewrite (multiple `Internal` errors — uncertain, the
  storage commit may be durable). Every refusal case has a nearby valid
  control; all calls are recorded so assertions can see exactly which
  operations ran (and which must NOT have run).
  """

  use ExUnit.Case, async: true

  alias DrinkMeAlice.Migrations

  @m1 %{version: 1, name: "audit", statements: ["DEFINE TABLE audit"]}
  @m2 %{version: 2, name: "second", statements: ["DEFINE TABLE second"]}

  # -- wire-shape helpers ------------------------------------------------------

  defp ok(result \\ nil), do: %{"status" => "OK", "result" => result}

  defp not_executed,
    do: %{
      "status" => "ERR",
      "kind" => "Query",
      "details" => %{"kind" => "NotExecuted"},
      "result" => "Query not executed"
    }

  defp cancelled,
    do: %{
      "status" => "ERR",
      "kind" => "Query",
      "details" => %{"kind" => "Cancelled"},
      "result" => "Cancelled"
    }

  defp real_err(msg), do: %{"status" => "ERR", "result" => msg}

  # The 3.0.5 commit-failure rewrite: every in-tx entry becomes a real
  # Internal error ("Query not executed: …"), COMMIT is omitted, the
  # RETURN shifts — and the storage commit may already be durable.
  defp internal_rewrite,
    do: %{"status" => "ERR", "result" => "Query not executed: grouped WAL sync failed"}

  # Full-length observed commit for a `BEGIN …; in-tx…; COMMIT; RETURN`
  # script of `n` statements ending in the given decision map.
  defp committed(n, decision) do
    [ok(nil)] ++
      List.duplicate(ok([]), n - 3) ++ [ok(nil), %{"status" => "OK", "result" => decision}]
  end

  # Qualified statement abort: reply one short, every in-tx entry before
  # the real error a NotExecuted marker (none after it in this shape).
  defp aborted(n, msg) do
    markers_before = List.duplicate(not_executed(), max(n - 4, 0))
    [ok(nil)] ++ markers_before ++ [real_err(msg)] ++ [ok(nil)]
  end

  # Commit-failure rewrite: reply one short, EVERY in-tx entry a real
  # Internal error, COMMIT omitted, RETURN shifted.
  defp commit_failure(n) do
    [ok(nil)] ++ List.duplicate(internal_rewrite(), n - 3) ++ [ok(nil)]
  end

  # -- the scripted peer -------------------------------------------------------

  defp classify(sql) do
    cond do
      String.starts_with?(sql, "DEFINE TABLE IF NOT EXISTS schema_migration") -> :bootstrap
      String.contains?(sql, "state = \"failed\"") -> :mark
      String.contains?(sql, "migration_lock:main SET owner") -> :lock
      String.starts_with?(sql, "SELECT version, checksum") -> :state
      String.contains?(sql, "DELETE FROM migration_lock") -> :release
      String.contains?(sql, "CREATE schema_migration CONTENT") -> :apply
      true -> :other
    end
  end

  defp statement_count(sql) do
    sql |> String.split(";\n") |> Enum.reject(&(&1 in ["", ";"])) |> length()
  end

  defp scripted_peer(opts \\ []) do
    test = self()

    fn sql, vars ->
      kind = classify(sql)
      send(test, {:call, kind})

      case {kind, Keyword.get(opts, kind, :ok)} do
        {_, {:fail, reason}} ->
          {:error, reason}

        {_, :crash} ->
          raise "scripted query crash"

        {kind, :empty} when kind in [:bootstrap, :lock, :release, :state] ->
          {:ok, []}

        {:bootstrap, :ok} ->
          {:ok, List.duplicate(ok([]), statement_count(sql))}

        {:bootstrap, :malformed} ->
          # R-03 counterexample: exact count, but entries without a
          # `result` slot.
          {:ok, List.duplicate(%{"status" => "OK"}, statement_count(sql))}

        {:lock, :ok} ->
          {:ok, committed(6, %{"decision" => "acquired"})}

        {:lock, :locked} ->
          # THROW of the lock tag: a qualified single-error abort, one
          # entry short.
          {:ok,
           [
             ok(nil),
             not_executed(),
             real_err("Incorrect sensitive value: bf:migration_locked"),
             cancelled(),
             ok(nil)
           ]}

        {:lock, :commit_failure} ->
          {:ok, commit_failure(6)}

        {:state, :ok} ->
          {:ok, [%{"status" => "OK", "result" => Keyword.get(opts, :state_rows, [])}]}

        {:state, :malformed} ->
          {:ok, [%{"status" => "OK"}]}

        {:apply, :ok} ->
          {:ok,
           committed(statement_count(sql), %{"decision" => "applied", "version" => vars["v"]})}

        {:apply, :throw_abort} ->
          {:ok, aborted(statement_count(sql), "Incorrect sensitive value: abort for test")}

        {:apply, :commit_failure} ->
          {:ok, commit_failure(statement_count(sql))}

        {:apply, :truncated} ->
          # Shortened but all-OK: not a qualified silhouette — uncertain.
          n = statement_count(sql)
          {:ok, [ok(nil) | List.duplicate(ok(nil), n - 3)] ++ [ok(nil)]}

        {:apply, :malformed} ->
          # R-03 counterexample: full length, decision looks right, but
          # the pre-commit entries carry no `result` slot.
          n = statement_count(sql)

          {:ok,
           [ok(nil)] ++
             List.duplicate(%{"status" => "OK"}, n - 3) ++
             [
               ok(nil),
               %{"status" => "OK", "result" => %{"decision" => "applied", "version" => vars["v"]}}
             ]}

        {:release, :ok} ->
          {:ok, [%{"status" => "OK", "result" => []}]}

        {:release, :malformed} ->
          {:ok, [%{"status" => "OK"}]}

        {:other, _} ->
          {:ok, [%{"status" => "OK", "result" => nil}]}
      end
    end
  end

  defp drain_calls do
    receive do
      {:call, k} -> [k | drain_calls()]
    after
      0 -> []
    end
  end

  # -- valid controls ----------------------------------------------------------

  test "valid control: observed commits apply, verify and release" do
    assert :ok = Migrations.apply_all(scripted_peer(), "owner_a", [@m1, @m2])

    assert drain_calls() ==
             [:bootstrap, :lock, :state, :apply, :state, :apply, :release]

    assert drain_calls() == []
  end

  test "qualified THROW abort is a definite failure with a failed mark" do
    assert {:error, {:statement_err, msg}} =
             Migrations.apply_all(scripted_peer(apply: :throw_abort), "owner_a", [@m1])

    assert msg =~ "abort for test"
    kinds = drain_calls()
    # The mark is written ONLY because the abort is qualified, and the
    # lock is released: a definite failure finalizes normally.
    assert kinds == [:bootstrap, :lock, :state, :apply, :mark, :release]
  end

  # -- R-01: uncertainty is never a failure, never overwritten ------------------

  test "commit-failure rewrite is UNCERTAIN: no mark, no release, lock retained (R-01)" do
    # The reviewer's probe: the same commit-failure reply Outcome calls
    # inconclusive must not become a failed marker plus a released lock.
    assert {:error, {:lock_retained, {:migration_unknown, 1, {:commit_result_unknown, _}}}} =
             Migrations.apply_all(scripted_peer(apply: :commit_failure), "owner_a", [@m1])

    kinds = drain_calls()
    # The decisive facts: no failure mark written, no release attempted.
    refute :mark in kinds
    refute :release in kinds
    assert List.last(kinds) == :apply
  end

  test "an all-OK shortened apply reply is uncertain, not a truncated failure (R-01)" do
    assert {:error, {:lock_retained, {:migration_unknown, 1, :shortened_shape_unqualified}}} =
             Migrations.apply_all(scripted_peer(apply: :truncated), "owner_a", [@m1])

    kinds = drain_calls()
    refute :mark in kinds
    refute :release in kinds
  end

  test "empty lock reply is uncertain acquisition, not a clean refusal (R-01)" do
    assert {:error, {:lock_state_unknown, {:unexpected_result_count, 0, 6}}} =
             Migrations.apply_all(scripted_peer(lock: :empty), "owner_a", [@m1])

    assert drain_calls() == [:bootstrap, :lock]
  end

  test "a commit-failure lock acquisition is uncertain (R-01)" do
    assert {:error, {:lock_state_unknown, {:commit_result_unknown, _}}} =
             Migrations.apply_all(scripted_peer(lock: :commit_failure), "owner_a", [@m1])

    assert drain_calls() == [:bootstrap, :lock]
  end

  test "transport loss during the run keeps the no-release rule and says so" do
    query = scripted_peer(apply: {:fail, {:lost, :timeout}})

    assert {:error, {:lock_release_unproven, {:run_error, {:lost, :timeout}, :transport_lost}}} =
             Migrations.apply_all(query, "owner_a", [@m1])

    kinds = drain_calls()
    refute :release in kinds
    assert List.last(kinds) == :apply
  end

  test "transport loss during lock acquisition reports unknown lock state" do
    assert {:error, {:lock_state_unknown, :closed}} =
             Migrations.apply_all(scripted_peer(lock: {:fail, {:lost, :closed}}), "owner_a", [@m1])

    assert drain_calls() == [:bootstrap, :lock]
  end

  test "a lock held by another owner is a definite, qualified refusal" do
    assert {:error, :locked} =
             Migrations.apply_all(scripted_peer(lock: :locked), "owner_a", [@m1])

    assert drain_calls() == [:bootstrap, :lock]
  end

  # -- R-02: callback and query crashes still finalize --------------------------

  test "a crashing vars callback is a typed error AND the lock is released (R-02)" do
    crashing = Map.put(@m1, :vars, fn _opts -> raise "vars boom" end)

    assert {:error, {:callback_crash, RuntimeError}} =
             Migrations.apply_all(scripted_peer(), "owner_a", [crashing])

    assert drain_calls() == [:bootstrap, :lock, :state, :release]
  end

  test "a crashing query function during apply is uncertain: no mark, lock retained (R-02)" do
    assert {:error, {:lock_retained, {:migration_unknown, 1, {:query_crashed, RuntimeError}}}} =
             Migrations.apply_all(scripted_peer(apply: :crash), "owner_a", [@m1])

    kinds = drain_calls()
    refute :mark in kinds
    refute :release in kinds
  end

  test "a crashing bootstrap query is a typed error before any lock exists (R-02)" do
    assert {:error, {:query_crashed, RuntimeError}} =
             Migrations.apply_all(scripted_peer(bootstrap: :crash), "owner_a", [@m1])

    assert drain_calls() == [:bootstrap]
  end

  test "a crashing release is surfaced as unproven (R-02)" do
    assert {:error, {:lock_release_unproven, {:run_ok, {:release_crashed, RuntimeError}}}} =
             Migrations.apply_all(scripted_peer(release: :crash), "owner_a", [@m1])

    assert :release in drain_calls()
  end

  # -- R-03: malformed success entries are not evidence -------------------------

  test "bootstrap entries without result slots are refused (R-03)" do
    assert {:error, {:bad_statement, :malformed_entry}} =
             Migrations.apply_all(scripted_peer(bootstrap: :malformed), "owner_a", [@m1])

    assert drain_calls() == [:bootstrap]
  end

  test "apply entries without result slots are uncertain even with a correct terminal (R-03)" do
    assert {:error, {:lock_retained, {:migration_unknown, 1, :bad_statement}}} =
             Migrations.apply_all(scripted_peer(apply: :malformed), "owner_a", [@m1])

    kinds = drain_calls()
    refute :mark in kinds
    refute :release in kinds
  end

  test "a malformed state reply is refused, never read as absence (R-03)" do
    assert {:error, {:bad_statement, :malformed_state_reply}} =
             Migrations.apply_all(scripted_peer(state: :malformed), "owner_a", [@m1])

    kinds = drain_calls()
    # The state read failed definitely (it submitted nothing): the run
    # stops and the lock IS released.
    assert kinds == [:bootstrap, :lock, :state, :release]
  end

  test "a malformed release reply is unproven (R-03)" do
    assert {:error, {:lock_release_unproven, {:run_ok, {:release_shape, :malformed}}}} =
             Migrations.apply_all(scripted_peer(release: :malformed), "owner_a", [@m1])
  end

  test "empty success arrays cannot prove bootstrap (A-06 original)" do
    assert {:error, {:incomplete_reply, 0, 12}} =
             Migrations.apply_all(scripted_peer(bootstrap: :empty), "owner_a", [@m1])

    assert drain_calls() == [:bootstrap]
  end

  test "an empty release reply is equally unproven" do
    assert {:error, {:lock_release_unproven, {:run_ok, {:incomplete_reply, 0, 1}}}} =
             Migrations.apply_all(scripted_peer(release: :empty), "owner_a", [@m1])
  end

  test "a checksum-mismatch state read keeps its dirty refusal" do
    assert {:error, {:dirty, 1, {:checksum_mismatch, "stale"}}} =
             Migrations.apply_all(
               scripted_peer(state_rows: [%{"state" => "applied", "checksum" => "stale"}]),
               "owner_a",
               [@m1]
             )

    assert :release in drain_calls()
  end

  test "check/1 refuses non-map members with a typed error, never a raise" do
    assert {:error, {:invalid_migrations, :not_a_map}} = Migrations.check([@m1, :not_a_map])
    assert {:error, {:invalid_migrations, :not_a_map}} = Migrations.check(["DEFINE TABLE x"])
    assert :ok = Migrations.check([@m1, @m2])
  end
end
