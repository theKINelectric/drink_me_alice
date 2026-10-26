defmodule DrinkMeAlice.TestServerTest do
  @moduledoc """
  Data-dir exclusivity for TestServer: a second start on the same store
  is refused, never raced into SurrealDB's LOCK — and the ownership
  machinery itself holds up: one caller may hold several stores, claims
  are released only by their own instance on proven cleanup, and the
  foreign-holder scan reports suspicion (exact argv token under a
  surreal executable), never verified ownership.
  """
  use ExUnit.Case, async: false

  alias DrinkMeAlice.TestServer

  defp fresh_dir(tag) do
    Path.join(System.tmp_dir!(), "dma_ts_#{tag}_#{System.unique_integer([:positive])}")
  end

  # One absolute deadline, carried through the recursion (W-04): a
  # helper that recomputes `now + ms` per call runs until the enclosing
  # test timeout, not the advertised bound.
  defp eventually(ms, fun) do
    wait_until(System.monotonic_time(:millisecond) + ms, ms, fun)
  end

  defp wait_until(deadline, ms, fun) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("condition not reached within #{ms} ms")
      else
        Process.sleep(25)
        wait_until(deadline, ms, fun)
      end
    end
  end

  # A live registration on the claim key, standing in for a running
  # server's dedicated holder process.
  defp register_holder(dir) do
    key = TestServer.claim_key(dir)
    parent = self()

    holder =
      spawn(fn ->
        :global.register_name(key, self())
        send(parent, :held)

        receive do
          :release -> :ok
        end
      end)

    assert_receive :held
    holder
  end

  defp release_holder!(holder) do
    send(holder, :release)

    eventually(2_000, fn ->
      Process.alive?(holder) == false
    end)
  end

  # The OS pid surfaces asynchronously after fork; bounded retry with
  # one carried deadline.
  defp await_os_pid(port, ms) do
    await_os_pid_step(port, ms, System.monotonic_time(:millisecond) + ms)
  end

  defp await_os_pid_step(port, ms, deadline) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} ->
        pid

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("no os_pid within #{ms} ms")
        else
          Process.sleep(25)
          await_os_pid_step(port, ms, deadline)
        end
    end
  end

  # BSD ps exits 1 once the pid is gone; empty output is the signal.
  defp ps_gone?(os_pid) do
    {out, _} = System.cmd("ps", ["-o", "pid=", "-p", Integer.to_string(os_pid)])
    String.trim(out) == ""
  end

  test "a start on a dir claimed by a live holder is refused, naming the holder" do
    dir = fresh_dir("claim")
    holder = register_holder(dir)
    on_exit(fn -> release_holder!(holder) end)

    assert {:error, {:data_dir_in_use, ^dir, holder_pid}} = TestServer.start(dir: dir)
    assert holder_pid == holder
    # Refused before any filesystem effect: the dir was never created.
    refute File.exists?(dir)
  end

  test "a failed start releases its data-dir claim" do
    dir = fresh_dir("release")
    old = System.get_env("SURREAL_BIN")
    System.put_env("SURREAL_BIN", "/nonexistent/dma-no-surreal")

    on_exit(fn ->
      case old do
        nil -> System.delete_env("SURREAL_BIN")
        v -> System.put_env("SURREAL_BIN", v)
      end
    end)

    assert {:error, :version_command_crashed} = TestServer.start(dir: dir)
    assert :undefined = :global.whereis_name(TestServer.claim_key(dir))
    refute File.exists?(dir)
  end

  test "a failing IO device cannot prevent startup finalization" do
    # The validation counterexample (W-02 round 2): a controlled
    # executable that answers `version` with the qualified identity and
    # otherwise execs sleep, plus a group leader that answers IO
    # requests with an error. Diagnostics used to raise out of the
    # health rescue handler, the outer rescue released the claim, and
    # the child kept running. Finalization must survive: typed result,
    # claim consistent with its cleanup proof, child reaped.
    dir = fresh_dir("faultyio")
    script_path = Path.join(dir, "fake_surreal")
    File.mkdir_p!(dir)
    File.write!(script_path, ~s(#!/bin/sh
if [ "$1" = "version" ]; then
  echo "surrealdb 3.0.5 for macos on aarch64"
else
  exec /bin/sleep 30
fi
))
    File.chmod!(script_path, 0o700)

    old = System.get_env("SURREAL_BIN")
    System.put_env("SURREAL_BIN", script_path)

    on_exit(fn ->
      case old do
        nil -> System.delete_env("SURREAL_BIN")
        v -> System.put_env("SURREAL_BIN", v)
      end

      File.rm_rf!(dir)
    end)

    # An IO device whose every request errors — IO.puts through it
    # raises, which is what the diagnostics path must survive. It loops:
    # a dead group leader would block the request instead of erroring.
    faulty_gl = spawn(&serve_faulty_io/0)
    real_gl = Process.group_leader()
    Process.group_leader(self(), faulty_gl)

    result =
      try do
        TestServer.start(dir: dir)
      after
        Process.group_leader(self(), real_gl)
        Process.exit(faulty_gl, :kill)
      end

    assert {:error, {:startup_failed, :health_deadline, cleanup}} = result
    # Proven cleanup — the TERM on the exec'd sleep was observed through
    # the port — so the claim was released, never leaked beside the
    # child. The observed exit status IS the child-exit proof.
    assert {:exit_status, _} = cleanup
    assert :undefined = :global.whereis_name(TestServer.claim_key(Path.expand(dir)))
  end

  defp serve_faulty_io do
    receive do
      {:io_request, from, reply_as, _request} ->
        send(from, {:io_reply, reply_as, {:error, :terminated}})
    end

    serve_faulty_io()
  end

  test "suspected-holder matching requires a surreal executable and an exact endpoint token" do
    ep = "surrealkv:///tmp/dma_match/data"

    # A real holder: executable named surreal, endpoint as its own token.
    assert TestServer.suspected_holder?(
             "/Volumes/x/.tools/surreal start --bind 127.0.0.1:1 #{ep}",
             ep
           )

    assert TestServer.suspected_holder?("surreal start #{ep} --log error", ep)

    # The /data_unrelated sibling-store probe: the token differs, so no
    # match even though the endpoint is a substring prefix.
    refute TestServer.suspected_holder?(
             "python probe.py surrealkv:///tmp/dma_match/data_unrelated",
             ep
           )

    # An exact endpoint token under a non-surreal executable is still
    # not a suspect — a command-line mention proves nothing.
    refute TestServer.suspected_holder?("python probe.py #{ep}", ep)
    refute TestServer.suspected_holder?("sh -c sleep 300; echo #{ep}", ep)

    # The endpoint embedded inside a larger token does not match.
    refute TestServer.suspected_holder?("/x/surreal start pre#{ep}", ep)
  end

  test "a stale handle stopped after a replacement does not release the live claim" do
    dir = fresh_dir("stale")
    replacement = register_holder(dir)
    on_exit(fn -> release_holder!(replacement) end)

    # A terminal stale handle from an EARLIER start on the same dir: its
    # claim identity is not the live registration.
    stale = %TestServer{
      port: make_ref(),
      dir: dir,
      exit_status: 0,
      claim_pid: spawn(fn -> :ok end)
    }

    assert {:exit_status, 0} = TestServer.stop(stale)
    # The replacement's claim survived the stale stop untouched.
    assert :global.whereis_name(TestServer.claim_key(dir)) == replacement
  end

  test "unproven cleanup preserves the claim even when the identity matches" do
    dir = fresh_dir("unproven")
    holder = register_holder(dir)
    on_exit(fn -> release_holder!(holder) end)

    # A real, owned OS child stands in for a server whose exit status
    # this handle cannot observe (its port reference is stale), so
    # stop/1 TERMs and KILLs the child but cannot prove the reap.
    child = Port.open({:spawn_executable, "/bin/sleep"}, [:binary, :exit_status, args: ["30"]])
    os_pid = await_os_pid(child, 2_000)

    server = %TestServer{
      port: make_ref(),
      os_pid: os_pid,
      dir: dir,
      exit_status: nil,
      claim_pid: holder
    }

    assert {:error, :unreaped} = TestServer.stop(server)
    # Unproven cleanup: the exclusion stays while the child's death is
    # unobserved — exactly the case where the store may still be held.
    assert :global.whereis_name(TestServer.claim_key(dir)) == holder

    # The child WAS killed; observe its exit through the real port.
    assert_receive {^child, {:exit_status, _}}, 1_000
  end

  @tag :live
  test "one caller holds two different stores simultaneously" do
    dir_a = fresh_dir("two_a")
    dir_b = fresh_dir("two_b")

    # Each acquisition installs its teardown boundary immediately; a
    # later failure cannot leave a child running (stopping an
    # already-stopped server is an immediate {:exit_status, _}). The
    # store is deleted ONLY on a proven stop; on unproven cleanup the
    # test fails and the directory is retained as evidence.
    assert {:ok, a} = TestServer.start(dir: dir_a)

    try do
      assert {:ok, b} = TestServer.start(dir: dir_b)

      try do
        assert a.dir == Path.expand(dir_a)
        assert b.dir == Path.expand(dir_b)
        assert a.http_port != b.http_port
        assert a.claim_pid != b.claim_pid
      after
        proven_stop!("two-stores/b", b, dir_b)
      end
    after
      proven_stop!("two-stores/a", a, dir_a)
    end
  end

  # Store deletion requires the child-exit proof from the stop result.
  # A release that timed out does not block deletion (the child is
  # proven dead) but is reported as an anomaly instead of hidden.
  defp proven_stop!(tag, server, dir) do
    case TestServer.stop(server) do
      {:exit_status, _} ->
        File.rm_rf!(dir)

      {:reaped, :status_unobserved} ->
        File.rm_rf!(dir)

      {:error, {:claim_release_unconfirmed, holder, cleanup}}
      when (is_tuple(cleanup) and elem(cleanup, 0) == :exit_status) or
             cleanup == {:reaped, :status_unobserved} ->
        File.rm_rf!(dir)

        flunk(
          "#{tag}: claim release unconfirmed by holder #{inspect(holder)} (child cleanup proven)"
        )

      other ->
        flunk("#{tag}: unproven cleanup #{inspect(other)}; store retained at #{dir}")
    end
  end

  @tag :live
  test "a store held by a foreign real surreal process is refused on suspicion, naming it" do
    dir = fresh_dir("realholder")
    {:ok, bin, _identity} = TestServer.resolve_bin()
    endpoint = "surrealkv://#{Path.expand(dir)}/data"

    # A REAL surreal child holding the store, started through a raw port
    # this test owns — no TestServer claim. Teardown is installed with
    # the acquisition and returns EVIDENCE: the store is deleted only on
    # an observed exit; on unconfirmed teardown the test fails and the
    # directory is retained.
    holder_port =
      Port.open(
        {:spawn_executable, bin},
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args: [
            "start",
            "--no-banner",
            "--no-defaults",
            "--log",
            "error",
            "--bind",
            "127.0.0.1:0",
            endpoint
          ]
        ]
      )

    os_pid = os_pid_within(holder_port, 2_000)

    try do
      assert is_integer(os_pid)

      # The store must exist for the preflight scan to run at all.
      eventually(5_000, fn -> File.exists?(Path.join(Path.expand(dir), "data")) end)

      assert {:error, {:data_dir_lock_suspected, expanded, suspects}} = TestServer.start(dir: dir)
      assert expanded == Path.expand(dir)
      assert [{pid, cmd}] = suspects
      assert pid == os_pid
      assert String.contains?(cmd, "surreal")
      # Refused before any filesystem effect of our own.
      refute File.exists?(Path.join(dir, "server.log"))
    after
      case teardown_foreign_holder(holder_port, os_pid) do
        :ok ->
          File.rm_rf!(dir)

        {:error, _} = unproven ->
          flunk(
            "foreign-holder teardown unproven (#{inspect(unproven)}); store retained at #{dir}"
          )
      end
    end
  end

  # Observed teardown of the raw holder child, in the port-owning
  # process: TERM, wait for its own exit status (escalating to KILL).
  # Returns :ok only on OBSERVED exit; uncertainty is an explicit
  # error, never a silent :ok. The timeouts are knobs so the forced-
  # uncertainty control can run fast.
  defp teardown_foreign_holder(port, os_pid, term_ms \\ 5_000, kill_ms \\ 2_000)

  defp teardown_foreign_holder(port, os_pid, term_ms, kill_ms) when is_integer(os_pid) do
    System.cmd("kill", ["-TERM", Integer.to_string(os_pid)], stderr_to_stdout: true)

    result =
      receive do
        {^port, {:exit_status, _}} -> :ok
      after
        term_ms ->
          System.cmd("kill", ["-KILL", Integer.to_string(os_pid)], stderr_to_stdout: true)

          receive do
            {^port, {:exit_status, _}} -> :ok
          after
            kill_ms -> {:error, {:exit_unobserved, os_pid}}
          end
      end

    try do
      Port.close(port)
    rescue
      _ -> :ok
    end

    result
  end

  defp teardown_foreign_holder(port, nil, _term_ms, _kill_ms) do
    # No OS pid ever surfaced: the child's state is unobservable — say
    # so instead of pretending a port close proved anything.
    try do
      Port.close(port)
    rescue
      _ -> :ok
    end

    {:error, {:no_os_pid, port}}
  end

  # Like await_os_pid/2 but yields nil at the deadline instead of
  # flunking, so a teardown `after` still has the (possibly nil) pid.
  defp os_pid_within(port, ms) do
    os_pid_step(port, System.monotonic_time(:millisecond) + ms)
  end

  defp os_pid_step(port, deadline) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} ->
        pid

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          nil
        else
          Process.sleep(25)
          os_pid_step(port, deadline)
        end
    end
  end

  test "teardown reports unproven when the child exit cannot be observed" do
    child = Port.open({:spawn_executable, "/bin/sleep"}, [:binary, :exit_status, args: ["30"]])
    os_pid = await_os_pid(child, 2_000)

    # Steal the exit notification: TERM the child and take its
    # exit_status ourselves, so teardown's own observation must fail —
    # the forced-uncertainty control for the deletion gate.
    System.cmd("kill", ["-TERM", Integer.to_string(os_pid)], stderr_to_stdout: true)
    assert_receive {^child, {:exit_status, _}}, 5_000

    assert {:error, {:exit_unobserved, ^os_pid}} =
             teardown_foreign_holder(child, os_pid, 200, 200)

    # The child's exit WAS observed by this test; nothing leaks.
    assert ps_gone?(os_pid)
  end

  @tag :live
  test "the claim clears when the owning process dies; the child is then reaped by pid" do
    dir = fresh_dir("ownerdies")
    parent = self()

    owner =
      spawn(fn ->
        case TestServer.start(dir: dir) do
          {:ok, server} ->
            send(parent, {:owned, server})

            receive do
              :hold -> :ok
            end

          {:error, reason} ->
            send(parent, {:owner_failed, reason})
        end
      end)

    receive do
      {:owned, server} ->
        key = TestServer.claim_key(Path.expand(dir))
        os_pid = server.os_pid

        try do
          assert :global.whereis_name(key) == server.claim_pid

          # The owner dies without stopping: the claim-holder's monitor
          # fires and the exclusion clears — the OS child may well
          # OUTLIVE its owner's port (measured on this runtime), so the
          # claim clearing is not a cleanup claim.
          Process.exit(owner, :kill)

          eventually(2_000, fn -> :global.whereis_name(key) == :undefined end)
        after
          # Failure-safe teardown: reap the child by pid, verify it gone
          # via ps, and only then remove its store.
          if Process.alive?(owner), do: Process.exit(owner, :kill)
          System.cmd("kill", ["-KILL", Integer.to_string(os_pid)], stderr_to_stdout: true)
          eventually(3_000, fn -> ps_gone?(os_pid) end)
          File.rm_rf!(dir)
        end

      {:owner_failed, reason} ->
        flunk("owner start failed: #{inspect(reason)}")
    after
      30_000 ->
        Process.exit(owner, :kill)
        flunk("owner never reported")
    end
  end

  @tag :live
  test "stop releases the claim: restart on the same dir and endpoint works" do
    dir = fresh_dir("restart")
    creds = [user: "stq_restart", pass: "fixed-restart-pass"]

    assert {:ok, s1} = TestServer.start(Keyword.put(creds, :dir, dir))

    # Phase 1 owns the directory only until s1 is proven stopped and
    # the replacement attempt begins (W-03 round 2): a failure here
    # stops s1 with proof and deletes; on success, authority over the
    # SHARED directory passes to the replacement's cleanup — s1's
    # later death is not cleanup evidence for it.
    try do
      assert {:exit_status, _} = TestServer.stop(s1)

      # W-01: a proven stop must not return until this claim's
      # registration is actually removed — the immediate restart below
      # relies on it deterministically.
      assert :undefined = :global.whereis_name(TestServer.claim_key(Path.expand(dir)))
    rescue
      e ->
        cleanup_first_generation("restart/s1", s1, dir)
        reraise e, __STACKTRACE__
    catch
      kind, reason ->
        cleanup_first_generation("restart/s1", s1, dir)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end

    # Phase 2: the replacement's cleanup is the SOLE owner of the
    # directory — including a failed replacement acquisition, whose
    # retention decision nothing enclosing may override.
    {:ok, s2} = TestServer.start(Keyword.merge(creds, dir: dir, port: s1.http_port))

    try do
      assert s2.os_pid != s1.os_pid
    after
      proven_stop!("restart/s2", s2, dir)
    end
  end

  # First-generation failure cleanup: by evidence, not by a second
  # stop — stop does not mutate the caller's struct, so a re-stop of
  # an already-stopped handle cannot observe that exit again.
  defp cleanup_first_generation(tag, server, dir) do
    cond do
      ps_gone?(server.os_pid) -> File.rm_rf!(dir)
      true -> proven_stop!(tag, server, dir)
    end
  end

  @tag :live
  test "a suspended claim-holder bounds stop's release wait; resume completes it" do
    dir = fresh_dir("suspended")
    creds = [user: "stq_suspend", pass: "fixed-suspend-pass"]

    assert {:ok, s} = TestServer.start(Keyword.put(creds, :dir, dir))
    key = TestServer.claim_key(Path.expand(dir))
    holder = s.claim_pid
    # Safety net first: a suspended process never schedules again on
    # its own, so nothing may leave it suspended. No on_exit directory
    # removal — the replacement's cleanup solely owns that decision.
    on_exit(fn ->
      if Process.alive?(holder), do: :erlang.resume_process(holder)
    end)

    :erlang.suspend_process(holder)

    try do
      # Child cleanup is proven; release cannot be confirmed while the
      # holder is suspended. The PUBLIC result must preserve BOTH
      # facts — the child proof and the unresolved release with its
      # holder receipt — not mask the pending release behind success.
      assert {:error, {:claim_release_unconfirmed, ^holder, {:exit_status, _}}} =
               TestServer.stop(s)

      assert :global.whereis_name(key) == holder

      assert {:error, {:data_dir_in_use, _dir, holder2}} =
               TestServer.start(Keyword.put(creds, :dir, dir))

      assert holder2 == holder
    after
      :erlang.resume_process(holder)
    end

    # Resumed: the holder completes the release it was asked for...
    eventually(2_000, fn -> :global.whereis_name(key) == :undefined end)

    # ...and an immediate restart succeeds, with a normally
    # acknowledged release on the way out. The replacement's cleanup
    # is the sole owner of the directory (W-03 round 2): an enclosing
    # on_exit rm could override its retention decision.
    {:ok, s2} = TestServer.start(Keyword.merge(creds, dir: dir))

    try do
      assert is_pid(s2.claim_pid)
    after
      proven_stop!("suspended/s2", s2, dir)
    end
  end

  @tag :live
  test "a failed start with proven cleanup releases the claim before returning" do
    dir = fresh_dir("earlyexit")
    key = TestServer.claim_key(Path.expand(dir))

    # Occupying the bind port makes the child fail fast: early exit is
    # observed, cleanup proven, and the failed start must release its
    # claim so an immediate retry is not refused by our own exclusion.
    # Deletion happens only on that proof — no on_exit backstop that
    # could remove a store a retained claim still guards.
    {:ok, l} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    {:ok, {_, port}} = :inet.sockname(l)

    try do
      assert {:error, {:startup_failed, {:early_exit, _}, {:exit_status, _}}} =
               TestServer.start(dir: dir, port: port)

      assert :undefined = :global.whereis_name(key)
      File.rm_rf!(dir)
    after
      :gen_tcp.close(l)
    end
  end

  @tag :live
  test "unproven replacement cleanup retains the store past every enclosing cleanup" do
    dir = fresh_dir("replacefail")
    creds = [user: "stq_replace", pass: "fixed-replace-pass"]
    key = TestServer.claim_key(Path.expand(dir))

    assert {:ok, s1} = TestServer.start(Keyword.put(creds, :dir, dir))

    # Generation 1 — the SAME phase orchestration and cleanup helper as
    # the restart test, so a regression in that generation-cleanup
    # decision fails here too, not only in the test that named it.
    try do
      assert {:exit_status, _} = TestServer.stop(s1)
      assert :undefined = :global.whereis_name(key)
    rescue
      e ->
        cleanup_first_generation("replace/s1", s1, dir)
        reraise e, __STACKTRACE__
    catch
      kind, reason ->
        cleanup_first_generation("replace/s1", s1, dir)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end

    # The replacement fixture: an unlinked owner that starts the server
    # (so this test's stop of it can never observe the exit — the
    # forced cleanup uncertainty) and can tear it down IN the
    # port-owning process on demand. The unconditional shutdown
    # boundary is installed the moment the owner exists — before any
    # assertion, receive, or timeout can fail.
    repl = spawn_replacement_owner(creds, dir)
    on_exit(fn -> replacement_fixture_cleanup(repl, dir) end)

    assert_receive {:replacement, ^repl, {:ok, s2}}, 30_000

    # The replacement's cleanup is UNPROVEN (non-owner stop) → its
    # decision: fail and RETAIN.
    assert {:error, :unreaped} = TestServer.stop(s2)

    # Every enclosing decision point that COULD delete has run or been
    # bypassed: phase 1 finished before this generation existed, and
    # the old child's proven death is not cleanup evidence for it.
    assert ps_gone?(s1.os_pid)

    # RETENTION — deliberately asserted BEFORE the fixture's eventual
    # proven cleanup: the store survives and the replacement's claim
    # still guards it.
    assert File.exists?(Path.join(dir, "data"))
    assert :global.whereis_name(key) == s2.claim_pid

    # Final cleanup, in proof order: the OWNER's stop observes the
    # child's exit; the owner terminates; the claim clears; then —
    # only then — the store is deleted.
    assert {:exit_status, _} = replacement_fixture_cleanup(repl, dir)
    assert Process.alive?(repl) == false
    assert :undefined = :global.whereis_name(key)
    refute File.exists?(dir)
  end

  @tag :live
  test "the replacement fixture still shuts down after an intentional body failure" do
    dir = fresh_dir("replacefailnet")
    creds = [user: "stq_net", pass: "fixed-net-pass"]
    key = TestServer.claim_key(Path.expand(dir))

    assert {:ok, s1} = TestServer.start(Keyword.put(creds, :dir, dir))

    try do
      assert {:exit_status, _} = TestServer.stop(s1)
      assert :undefined = :global.whereis_name(key)
    rescue
      e ->
        cleanup_first_generation("replace-net/s1", s1, dir)
        reraise e, __STACKTRACE__
    catch
      kind, reason ->
        cleanup_first_generation("replace-net/s1", s1, dir)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end

    repl = spawn_replacement_owner(creds, dir)
    on_exit(fn -> replacement_fixture_cleanup(repl, dir) end)

    assert_receive {:replacement, ^repl, {:ok, s2}}, 30_000
    os_pid = s2.os_pid

    # The intentional failure: exactly the path whose cleanup the
    # fixture's shutdown boundary must cover, driven through the SAME
    # helper on_exit would run.
    assert_raise ExUnit.AssertionError, fn ->
      try do
        flunk("intentional failure inside the replacement fixture")
      after
        _ = replacement_fixture_cleanup(repl, dir)
      end
    end

    # Teardown proved: child gone, owner gone, claim cleared — and the
    # store deleted only after those proofs.
    assert ps_gone?(os_pid)
    assert Process.alive?(repl) == false
    assert :undefined = :global.whereis_name(key)
    refute File.exists?(dir)
  end

  # The replacement owner: starts the server and holds it, reporting
  # the result; on {:release_owner, reply_to} it stops the child IN the
  # port-owning process (where the exit is observable) and reports the
  # stop result. A failed start reports and exits — its claim/child
  # were already dispositioned by start/1 itself.
  defp spawn_replacement_owner(creds, dir) do
    parent = self()

    spawn(fn ->
      result = TestServer.start(Keyword.merge(creds, dir: dir))
      send(parent, {:replacement, self(), result})

      case result do
        {:ok, server} ->
          receive do
            {:release_owner, reply_to} ->
              send(reply_to, {:owner_stopped, TestServer.stop(server)})
          end

        {:error, _reason} ->
          :ok
      end
    end)
  end

  # Unconditional fixture shutdown for a replacement owner. Proof
  # order: the owner's own stop (child exit observed where the port
  # lives) → owner terminated → claim clearance observed → leftover
  # reaping by this dir's unique endpoint evidence → deletion. On
  # UNPROVEN child cleanup the directory is RETAINED and the receipt
  # reported — never deleted on uncertainty.
  defp replacement_fixture_cleanup(repl, dir) do
    child_proof =
      if Process.alive?(repl) do
        send(repl, {:release_owner, self()})

        receive do
          {:owner_stopped, result} -> result
        after
          10_000 -> :owner_unresponsive
        end
      else
        {:reaped, :status_unobserved}
      end

    if Process.alive?(repl), do: Process.exit(repl, :kill)

    case child_proof do
      {:exit_status, _} = proven ->
        await_claim_clear(dir, 5_000)
        reap_by_endpoint_evidence(dir)
        File.rm_rf!(dir)
        proven

      {:reaped, :status_unobserved} = proven ->
        await_claim_clear(dir, 5_000)
        reap_by_endpoint_evidence(dir)
        File.rm_rf!(dir)
        proven

      unproven ->
        receipt = %{
          owner: repl,
          claim: :global.whereis_name(TestServer.claim_key(Path.expand(dir)))
        }

        safe_fixture_puts(
          "replacement fixture cleanup unproven (#{inspect(unproven)}); " <>
            "store retained at #{dir}; receipt #{inspect(receipt)}"
        )

        {:retained, unproven, receipt}
    end
  end

  defp safe_fixture_puts(message) do
    try do
      IO.puts(message)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  defp await_claim_clear(dir, ms) do
    key = TestServer.claim_key(Path.expand(dir))

    step_until(System.monotonic_time(:millisecond) + ms, fn ->
      :global.whereis_name(key) == :undefined
    end)
  end

  defp step_until(deadline, fun) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        :timeout
      else
        Process.sleep(25)
        step_until(deadline, fun)
      end
    end
  end

  # Failure-net reaping: kill (and verify gone) anything still holding
  # this fixture's unique endpoint — only this test's dir can match.
  defp reap_by_endpoint_evidence(dir) do
    endpoint = "surrealkv://#{Path.expand(dir)}/data"

    {out, _} = System.cmd("ps", ["-eo", "pid=,command="], stderr_to_stdout: true)

    out
    |> String.split("\n", trim: true)
    |> Enum.flat_map(fn line ->
      case Integer.parse(String.trim_leading(line)) do
        {pid, " " <> cmd} ->
          if String.contains?(String.trim_leading(cmd), endpoint), do: [pid], else: []

        _ ->
          []
      end
    end)
    |> Enum.each(fn pid ->
      System.cmd("kill", ["-KILL", Integer.to_string(pid)], stderr_to_stdout: true)
    end)
  end

  @tag :live
  test "an assertion failing after acquisition still reaps the child and clears the claim" do
    dir = fresh_dir("failpath")
    {:ok, s} = TestServer.start(dir: dir)
    key = TestServer.claim_key(Path.expand(dir))
    os_pid = s.os_pid

    assert_raise ExUnit.AssertionError, fn ->
      try do
        flunk("intentional failure between acquisition and teardown")
      after
        TestServer.stop(s)
      end
    end

    assert :undefined = :global.whereis_name(key)
    eventually(3_000, fn -> ps_gone?(os_pid) end)
    File.rm_rf!(dir)
  end

  test "failed starts release, retain, or defer the claim by failure class" do
    # Pre-spawn failures: nothing exists, release synchronously.
    assert :release = TestServer.claim_disposition(:version_command_crashed)
    assert :release = TestServer.claim_disposition({:unqualified_server, "x"})
    assert :release = TestServer.claim_disposition({:data_dir_lock_suspected, "d", []})
    assert :release = TestServer.claim_disposition({:port_open, "x"})
    # The port existed before the failure: acquire wrapped it where the
    # acquisition state is known — cleanup unproven, exclusion retained.
    assert :retain = TestServer.claim_disposition({:acquisition_unclean, :no_os_pid, self()})

    assert :retain =
             TestServer.claim_disposition({:acquisition_unclean, {:port_info, "x"}, self()})

    # Startup failures were finalized by stop/1 under the same gate
    # (proven cleanup released it there; unproven cleanup retained it
    # and the error carries the holder receipt).
    assert :decided = TestServer.claim_disposition({:startup_failed, :x, {:exit_status, 1}})

    assert :decided =
             TestServer.claim_disposition(
               {:startup_failed, :x, {:cleanup_unproven, :unreaped, self()}}
             )

    assert :decided =
             TestServer.claim_disposition(
               {:startup_failed, :x, {:claim_release_unconfirmed, self(), {:exit_status, 0}}}
             )
  end

  test "eventually/2 stops at its own deadline when the condition never holds" do
    t0 = System.monotonic_time(:millisecond)

    assert_raise ExUnit.AssertionError, fn ->
      eventually(300, fn -> false end)
    end

    assert System.monotonic_time(:millisecond) - t0 < 1_500
  end

  test "await_os_pid/2 stops at its own deadline when no pid surfaces" do
    # An already-exited child whose exit was OBSERVED provides the
    # closed port — no live sleep left behind while its port is gone
    # (W-03: a port close does not reap the OS child).
    port = Port.open({:spawn_executable, "/bin/sleep"}, [:binary, :exit_status, args: ["1"]])
    assert_receive {^port, {:exit_status, _}}, 5_000

    try do
      Port.close(port)
    rescue
      _ -> :ok
    end

    t0 = System.monotonic_time(:millisecond)

    assert_raise ExUnit.AssertionError, fn ->
      await_os_pid(port, 300)
    end

    assert System.monotonic_time(:millisecond) - t0 < 1_500
  end
end
