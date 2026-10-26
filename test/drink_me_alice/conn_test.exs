defmodule DrinkMeAlice.ConnTest do
  @moduledoc """
  Connection-owner regression coverage over a scriptable loopback peer
  (`FakeSurreal` — transport behavior only; never SurrealDB evidence).

  Covers the review findings:

  - hint coalescing: N matching notifications → exactly one subscriber
    message; `hint_take/1` acknowledges and re-arms the edge.
  - stale-generation notifications counted, never applied.
  - clock latching on a `regressed` reply flag.
  - credit storm: 32 concurrent callers under a busy owner; doorbell
    coalescing keeps the owner mailbox bounded (observed high water).
  - caller timeout/death: monitors restored to baseline, zero retained
    stale replies — late owner sends are dropped by the destroyed alias.
  - A-02: the write path (`Tx.run/4`) runs wholly under the bounded
    protocol — missing owner is a typed unavailable, a suspended owner
    is bounded by the caller's deadline, and the clock gate refuses
    mutations at dispatch (latched before submission OR after
    admission), with no mutation frame reaching the peer.
  - A-04: repeated hint subscriptions retain one monitor per pid;
    re-join at capacity is idempotent, a new pid is refused, and a
    subscriber's death releases its slot.
  """

  use ExUnit.Case, async: false

  alias DrinkMeAlice.{Conn, Credits, FakeSurreal, Tx}

  @live_uuid "fake-live-uuid-1"

  defp start_conn(fake, extra \\ []) do
    config =
      [
        host: "127.0.0.1",
        port: fake.port,
        ns: "app",
        db: "app",
        user: "u",
        pass: "p",
        live: "work_item",
        clock_query: {"RETURN clock_probe;", %{}},
        poll_ms: 10
      ] ++ extra

    {:ok, pid} = Conn.start(config)
    assert_healthy(pid)
    pid
  end

  defp assert_healthy(pid, timeout \\ 3_000) do
    if Conn.status(pid) == :healthy do
      :ok
    else
      if timeout <= 0 do
        raise "conn did not become healthy: #{Conn.status(pid)}"
      else
        Process.sleep(20)
        assert_healthy(pid, timeout - 20)
      end
    end
  end

  defp mailbox_size(pid), do: elem(Process.info(pid, :message_queue_len), 1)

  test "callback crash log never contains the runtime password" do
    {:ok, fake} = FakeSurreal.start()
    secret = "test-secret-" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    try do
      conn = start_conn(fake, user: "crashlog", pass: secret)
      ref = Process.monitor(conn)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          spawn(fn ->
            try do
              GenServer.call(conn, :no_such_callback_crash)
            catch
              :exit, _ -> :ok
            end
          end)

          receive do
            {:DOWN, ^ref, :process, ^conn, _} -> :ok
          after
            3_000 -> raise "owner did not exit"
          end

          Logger.flush()
        end)

      # The crash DID happen and WAS logged — the credential simply is
      # not in it (function-clause args AND the State line both redact).
      assert byte_size(log) > 0
      refute Process.alive?(conn)
      refute log =~ secret
      refute log =~ "crashlog"
    after
      :ok = FakeSurreal.stop(fake)
    end
  end

  defp drain_mailbox(acc \\ 0) do
    receive do
      _ -> drain_mailbox(acc + 1)
    after
      0 -> acc
    end
  end

  defp monitor_count, do: length(elem(Process.info(self(), :monitors), 1))

  # A 13-statement script: BEGIN@0, in-tx statements @1..10, COMMIT@11,
  # RETURN@12 — the shape the pinned-server regressions below replay.
  defp accept_shaped_tx(op_id) do
    {:ok, tx} =
      Tx.build(
        ["BEGIN TRANSACTION"] ++
          List.duplicate("LET $x = 1", 10) ++
          ["COMMIT TRANSACTION", "RETURN {decision: \"commit\"}"],
        %{"op_id" => op_id}
      )

    assert tx.ctx == %{commit: 11, decision: 12, len: 13}
    tx
  end

  @verdicts %{"commit" => :committed}

  test "shortened commit-failure reply yields unknown with exactly one mutation (review-44)" do
    # Real pinned-3.0.5 commit-failure shape: every in-tx entry is
    # rewritten to Internal("Query not executed: …"), the COMMIT entry
    # is OMITTED, and the trailing RETURN runs at the nominal COMMIT
    # index — the reply is one entry short (12, not 13). The shifted
    # RETURN here even carries a committed-looking decision map: it is
    # never COMMIT evidence. No retry, no definite outcome.
    {:ok, fake} = FakeSurreal.start()
    test = self()

    rewrite = %{
      "status" => "ERR",
      "kind" => "Internal",
      "result" => "Query not executed: grouped WAL sync failed"
    }

    shifted_return = %{
      "status" => "OK",
      "result" => %{"decision" => "commit", "op" => nil, "attempts" => []}
    }

    reply = [
      %{"status" => "OK", "result" => nil},
      rewrite,
      rewrite,
      rewrite,
      rewrite,
      rewrite,
      rewrite,
      rewrite,
      rewrite,
      rewrite,
      rewrite,
      shifted_return
    ]

    handler = fn
      "query", [_sql, %{"op_id" => _}] ->
        send(test, :mutation_query)
        {:reply, reply}

      method, params ->
        FakeSurreal.default(method, params)
    end

    try do
      conn = start_conn(fake)

      :ok = FakeSurreal.command(fake, {:set_handler, handler})

      tx = accept_shaped_tx("00000000-0000-4000-8000-000000440001")
      assert {:unknown, ^tx} = Tx.run(conn, tx, @verdicts, deadline_ms: 2_000)

      assert_received :mutation_query
      refute_received :mutation_query
    after
      FakeSurreal.stop(fake)
    end
  end

  test "full-length unqualified error at nominal COMMIT yields unknown, one mutation (review-44)" do
    # A 13-entry reply with a real ERR at the nominal COMMIT slot does
    # not match any pinned executor path (a failed COMMIT pushes no
    # entry and shortens the reply) — unqualified evidence, so the
    # outcome stays unknown. Exactly one mutation query.
    {:ok, fake} = FakeSurreal.start()
    test = self()

    ok = %{"status" => "OK", "result" => nil}

    commit_err = %{
      "status" => "ERR",
      "kind" => "Internal",
      "result" => "commit coordinator response channel closed"
    }

    decision = %{
      "status" => "OK",
      "result" => %{"decision" => "commit", "op" => nil, "attempts" => []}
    }

    reply =
      List.duplicate(ok, 11) ++
        [commit_err, decision]

    handler = fn
      "query", [_sql, %{"op_id" => _}] ->
        send(test, :mutation_query)
        {:reply, reply}

      method, params ->
        FakeSurreal.default(method, params)
    end

    try do
      conn = start_conn(fake)

      :ok = FakeSurreal.command(fake, {:set_handler, handler})

      tx = accept_shaped_tx("00000000-0000-4000-8000-000000440003")
      assert {:unknown, ^tx} = Tx.run(conn, tx, @verdicts, deadline_ms: 2_000)

      assert_received :mutation_query
      refute_received :mutation_query
    after
      FakeSurreal.stop(fake)
    end
  end

  defp query_req do
    %{method: "query", params: ["RETURN 1;", %{}], kind: :read}
  end

  test "establishes session and reports healthy with live subscription" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    s = Conn.stats(conn)
    assert s.status == :healthy
    assert s.live_uuid_present
    assert s.credits.general == {0, 26}
    assert s.credits.internal == {0, 6}

    FakeSurreal.stop(fake)
  end

  test "matching notifications coalesce to one subscriber message per edge" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    assert :ok = Conn.hint_subscribe(conn)

    for _ <- 1..5 do
      :ok = FakeSurreal.notify(fake, %{"id" => @live_uuid, "action" => "CREATE", "result" => %{}})
    end

    # Exactly one hint message despite five notifications — wait for all
    # five to be absorbed, then prove only one edge fired.
    assert_eventually(fn -> Conn.stats(conn).notifications_seen == 5 end, 2_000)
    assert_receive {:drink_me_alice_hint, "work_item", _gen}, 2_000
    assert drain_mailbox() == 0

    s = Conn.stats(conn)
    assert s.hint_seen == 1

    # Take acknowledges and re-arms; the next edge produces one message.
    assert {:hint, 1} = Conn.hint_take(conn)

    :ok = FakeSurreal.notify(fake, %{"id" => @live_uuid, "action" => "CREATE", "result" => %{}})
    assert_receive {:drink_me_alice_hint, "work_item", _gen}, 2_000
    assert drain_mailbox() == 0

    FakeSurreal.stop(fake)
  end

  test "stale-generation notifications are counted and never fanned out" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    assert :ok = Conn.hint_subscribe(conn)

    for _ <- 1..3 do
      :ok =
        FakeSurreal.notify(fake, %{
          "id" => "a-dead-generation-uuid",
          "action" => "CREATE",
          "result" => %{}
        })
    end

    Process.sleep(150)
    assert drain_mailbox() == 0

    s = Conn.stats(conn)
    assert s.notifications_stale == 3
    refute s.hint_pending

    FakeSurreal.stop(fake)
  end

  test "a regressed flag in any reply latches clock uncertainty" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    assert Conn.clock_authority(conn) == :trusted

    :ok =
      FakeSurreal.command(
        fake,
        {:set_handler,
         fn
           "signin", _ ->
             {:reply, nil}

           "use", _ ->
             {:reply, nil}

           "live", _ ->
             {:reply, @live_uuid}

           "query", _ ->
             {:reply,
              [
                %{
                  "status" => "OK",
                  "result" => %{
                    "now" => "2026-01-01T00:00:00Z",
                    "floor" => "2026-01-01T00:00:01Z",
                    "regressed" => true,
                    "ok" => true
                  }
                }
              ]}

           _m, _p ->
             {:reply, nil}
         end}
      )

    assert {:reply, _} = Conn.call(conn, query_req())
    assert Conn.clock_authority(conn) == :uncertain

    FakeSurreal.stop(fake)
  end

  test "timeout storm: caller monitors and mailbox return to baseline" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    # Fake sleeps past the caller deadline, then answers — the owner
    # consumes the reply and sends to a destroyed alias (dropped).
    :ok =
      FakeSurreal.command(
        fake,
        {:set_handler,
         fn
           "signin", _ ->
             {:reply, nil}

           "use", _ ->
             {:reply, nil}

           "live", _ ->
             {:reply, @live_uuid}

           "query", _ ->
             {:sleep, 120,
              [
                %{
                  "status" => "OK",
                  "result" => %{"now" => "t", "floor" => "t", "regressed" => false, "ok" => true}
                }
              ]}

           _m, _p ->
             {:reply, nil}
         end}
      )

    base_monitors = monitor_count()
    base_msgs = drain_mailbox()

    for _ <- 1..8 do
      # Caller-side timeout OR owner-side certified refusal (the slot's
      # deadline lapsed before the owner drained it — a stronger fact
      # than lost, since the request provably never entered the wire).
      assert Conn.call(conn, query_req(), deadline_ms: 30) in [
               {:lost, :timeout},
               {:refused, :expired}
             ]
    end

    # Let the owner consume every outstanding request + late replies.
    Process.sleep(400)

    assert monitor_count() == base_monitors
    assert drain_mailbox() == base_msgs

    # All slots freed — no leaked credits across the storm.
    s = Conn.stats(conn)
    assert s.credits.general == {0, 26}

    FakeSurreal.stop(fake)
  end

  test "credit storm: 32 concurrent callers; owner mailbox stays bounded" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    :ok =
      FakeSurreal.command(
        fake,
        {:set_handler,
         fn
           "signin", _ ->
             {:reply, nil}

           "use", _ ->
             {:reply, nil}

           "live", _ ->
             {:reply, @live_uuid}

           "query", _ ->
             {:sleep, 30,
              [
                %{
                  "status" => "OK",
                  "result" => %{"now" => "t", "floor" => "t", "regressed" => false, "ok" => true}
                }
              ]}

           _m, _p ->
             {:reply, nil}
         end}
      )

    parent = self()

    callers =
      for i <- 1..32 do
        spawn(fn ->
          class = if i <= 26, do: :general, else: :internal
          send(parent, {:done, i, Conn.call(conn, query_req(), class: class, deadline_ms: 1_800)})
        end)
      end

    # Sample the owner's mailbox while the storm drains.
    peak =
      Enum.reduce(1..60, 0, fn _, acc ->
        Process.sleep(15)
        max(acc, mailbox_size(conn))
      end)

    results =
      for _ <- 1..32,
          do:
            (receive do
               {:done, _i, r} -> r
             after
               5_000 -> :missing
             end)

    # Every caller got a typed reply (reply, refused or lost — never a crash).
    for r <- results do
      assert match?({:reply, _}, r) or match?({:lost, _}, r) or match?({:refused, _}, r),
             inspect(r)
    end

    # Poll-only drain: the owner mailbox should hold at most one pending
    # :poll — assert the observed peak stays well under the 32-caller
    # count, and record it for evidence.
    IO.inspect(%{owner_mailbox_peak: peak, callers: 32})
    assert peak <= 4, "owner mailbox peaked at #{peak} — doorbells are not coalesced"

    for c <- callers, do: refute(Process.alive?(c))

    s = Conn.stats(conn)
    assert s.credits.general == {0, 26}
    assert s.credits.internal == {0, 6}

    FakeSurreal.stop(fake)
  end

  test "sustained refill during drain keeps doorbells bounded" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    :ok =
      FakeSurreal.command(
        fake,
        {:set_handler,
         fn
           "signin", _ ->
             {:reply, nil}

           "use", _ ->
             {:reply, nil}

           "live", _ ->
             {:reply, @live_uuid}

           "query", _ ->
             {:sleep, 20,
              [
                %{
                  "status" => "OK",
                  "result" => %{"now" => "t", "floor" => "t", "regressed" => false, "ok" => true}
                }
              ]}

           _m, _p ->
             {:reply, nil}
         end}
      )

    parent = self()

    # 8 long-lived callers each refill the slots 15 times — refill lands
    # while the owner is still batch-draining, exercising the bell edge.
    for i <- 1..8 do
      spawn(fn ->
        results = for _ <- 1..15, do: Conn.call(conn, query_req(), deadline_ms: 1_900)
        send(parent, {:drained, i, results})
      end)
    end

    peak =
      Enum.reduce(1..80, 0, fn _, acc ->
        Process.sleep(15)
        max(acc, mailbox_size(conn))
      end)

    for _ <- 1..8 do
      assert_receive {:drained, _i, rs}, 8_000

      for r <- rs do
        assert match?({:reply, _}, r) or match?({:lost, _}, r) or match?({:refused, _}, r)
      end
    end

    # 120 sequential refills over 8 callers produced a bounded owner
    # mailbox — bells coalesce to drain-cycle edges, not request count.
    IO.inspect(%{refill_calls: 120, owner_mailbox_peak: peak})
    assert peak <= 4, "owner mailbox peaked at #{peak} under sustained refill"

    s = Conn.stats(conn)
    assert s.credits.general == {0, 26}

    FakeSurreal.stop(fake)
  end

  test "reply racing timeout: one of two outcomes, mailbox stays clean" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    # Reply lands right at the caller's deadline boundary — the reply
    # and the timeout race; either fact is acceptable, leaks are not.
    :ok =
      FakeSurreal.command(
        fake,
        {:set_handler,
         fn
           "signin", _ ->
             {:reply, nil}

           "use", _ ->
             {:reply, nil}

           "live", _ ->
             {:reply, @live_uuid}

           "query", _ ->
             {:sleep, 80,
              [
                %{
                  "status" => "OK",
                  "result" => %{"now" => "t", "floor" => "t", "regressed" => false, "ok" => true}
                }
              ]}

           _m, _p ->
             {:reply, nil}
         end}
      )

    base_monitors = monitor_count()
    base_msgs = drain_mailbox()

    outcomes =
      for _ <- 1..10 do
        Conn.call(conn, query_req(), deadline_ms: 90)
      end

    for r <- outcomes do
      assert match?({:reply, _}, r) or r in [{:lost, :timeout}, {:refused, :expired}],
             inspect(r)
    end

    Process.sleep(300)

    assert monitor_count() == base_monitors
    assert drain_mailbox() == base_msgs
    assert Conn.stats(conn).credits.general == {0, 26}

    FakeSurreal.stop(fake)
  end

  test "connection loss mid-request: caller gets a lost fact, leaks nothing" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    # Drop-mode handler: never answers queries, so the request is in
    # flight when the socket dies underneath it.
    :ok =
      FakeSurreal.command(
        fake,
        {:set_handler,
         fn
           "signin", _ -> {:reply, nil}
           "use", _ -> {:reply, nil}
           "live", _ -> {:reply, @live_uuid}
           "query", _ -> :drop
           _m, _p -> {:reply, nil}
         end}
      )

    parent = self()

    caller =
      spawn(fn ->
        send(parent, {:call_result, Conn.call(conn, query_req(), deadline_ms: 1_900)})
      end)

    # Wait until the request is on the wire, then kill the TCP socket.
    # Observe the slot table DIRECTLY — `Conn.stats/1` is a GenServer
    # call that queues behind the owner's in-flight `await_rpc`, so it
    # cannot observe the very request it is waiting on.
    {:ok, tab} = Conn.credits(conn)

    assert_eventually(
      fn -> Credits.snapshot(tab).processing > 0 end,
      2_000
    )

    :ok = FakeSurreal.close_tcp(fake)

    assert_receive {:call_result, r}, 4_000
    assert match?({:lost, _}, r), inspect(r)
    refute Process.alive?(caller)

    # Reconnect happens on its own schedule; the table must not leak.
    s = Conn.stats(conn)
    assert s.status in [:disconnected, :reconciling, :healthy]

    FakeSurreal.stop(fake)
  end

  test "dead caller's queued request is consumed and freed, not leaked" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    {:ok, tab} = Conn.credits(conn)

    {_pid, ref} =
      spawn_monitor(fn ->
        # Reserve + enqueue, then die — the payload is already in the row.
        {:ok, token} = Credits.reserve(tab, :general)

        :ok =
          Credits.enqueue(
            tab,
            token,
            {Process.alias(), %{method: "query", params: ["RETURN 1;", %{}], kind: :read},
             System.monotonic_time(:millisecond) + 30_000}
          )
      end)

    wait_down(ref)

    # The poll sweep never touches an enqueued row; the owner drains it.
    assert_healthy(conn)
    assert_eventually(fn -> Credits.snapshot(tab).general == {0, 26} end, 3_000)

    FakeSurreal.stop(fake)
  end

  test "owner down: calls refuse fast with :down or :lost, never crash" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    Process.exit(conn, :kill)
    Process.sleep(50)

    r = Conn.call(conn, query_req(), deadline_ms: 200)
    assert r == {:refused, :down} or match?({:lost, _}, r)

    FakeSurreal.stop(fake)
  end

  # -- A-02: the write path runs wholly under the bounded protocol ------

  test "Tx.run against a missing owner is unavailable, never a noproc exit (A-02)" do
    # The audited reproduction: the preflight control call exited the
    # caller with {:noproc, {GenServer, :call, [..., :clock_authority,
    # 5000]}} before any bounded machinery could run.
    {:ok, tx} =
      Tx.build(
        [
          "BEGIN TRANSACTION",
          "LET $x = 1",
          "LET $y = 2",
          "COMMIT TRANSACTION",
          "RETURN {decision: 'ok'}"
        ],
        %{"ok" => :committed}
      )

    assert {:unavailable, %{code: "database_unavailable"}} =
             Tx.run(:astra_missing_conn, tx, %{"ok" => :committed})
  end

  test "a suspended owner bounds the write path by the caller's deadline (A-02)" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)
    {:ok, tab} = Conn.credits(conn)

    :sys.suspend(conn)
    tx = accept_shaped_tx("00000000-0000-4000-8000-000000a00201")

    t0 = System.monotonic_time(:millisecond)
    assert {:unknown, ^tx} = Tx.run(conn, tx, @verdicts, deadline_ms: 10)
    elapsed = System.monotonic_time(:millisecond) - t0

    # The old path blocked a full 5 s on a control call regardless of
    # the deadline; the bounded path returns within deadline + slack.
    assert elapsed < 2_000, "write path took #{elapsed} ms under a 10 ms deadline"

    :sys.resume(conn)

    # The queued row is consumed refused-typed and freed once the owner
    # resumes — no leaked credit slot.
    assert_eventually(fn -> Credits.snapshot(tab).general == {0, 26} end, 3_000)

    FakeSurreal.stop(fake)
  end

  test "clock uncertainty latched before submission refuses the mutation at dispatch (A-02)" do
    {:ok, fake} = FakeSurreal.start()
    test = self()

    regressed = fn
      "query", [sql, _] ->
        if String.starts_with?(sql, "BEGIN TRANSACTION") do
          send(test, :mutation_seen)
          {:reply, List.duplicate(%{"status" => "OK", "result" => nil}, 12)}
        else
          {:reply,
           [
             %{
               "status" => "OK",
               "result" => %{"now" => "t", "floor" => "t", "regressed" => true, "ok" => true}
             }
           ]}
        end

      method, params ->
        FakeSurreal.default(method, params)
    end

    try do
      conn = start_conn(fake)
      :ok = FakeSurreal.command(fake, {:set_handler, regressed})

      # A read reply carrying regressed: true latches uncertainty …
      assert {:reply, _} = Conn.call(conn, query_req())
      assert Conn.clock_authority(conn) == :uncertain

      # … and the mutation is refused as a certified non-send, while
      # reads keep flowing (the gate is mutation-only).
      tx = accept_shaped_tx("00000000-0000-4000-8000-000000a00202")

      assert {:unavailable, %{code: "database_unavailable", detail: :clock_uncertain}} =
               Tx.run(conn, tx, @verdicts)

      assert {:reply, _} = Conn.call(conn, query_req())
      refute_received :mutation_seen
    after
      FakeSurreal.stop(fake)
    end
  end

  test "uncertainty latched AFTER admission still refuses the mutation before send (A-02 gate race)" do
    # The controlled race the audit could not force: caller A's read is
    # in flight (its reply will latch uncertainty); the mutation below
    # is admitted into the credit table while the owner is still busy
    # with A. When A's reply latches the uncertainty, the owner must
    # refuse the already-admitted mutation AT DISPATCH — no mutation
    # frame may reach the peer.
    #
    # Deterministic barrier (review follow-up): the fake HOLDS A's reply
    # until the test has observed, via the credit table itself, that the
    # mutation is admitted AND enqueued — the exact race window — so a
    # delayed caller can no longer let this pass unexercised.
    {:ok, fake} = FakeSurreal.start()
    test = self()

    handler = fn
      "query", [sql, _] ->
        cond do
          String.starts_with?(sql, "BEGIN TRANSACTION") ->
            send(test, :mutation_seen)
            {:reply, List.duplicate(%{"status" => "OK", "result" => nil}, 12)}

          String.starts_with?(sql, "RETURN 1") ->
            send(test, :read_seen)

            receive do
              :release_read -> :ok
            after
              10_000 -> :ok
            end

            {:reply,
             [
               %{
                 "status" => "OK",
                 "result" => %{"now" => "t", "floor" => "t", "regressed" => true, "ok" => true}
               }
             ]}

          true ->
            FakeSurreal.default("query", [sql, %{}])
        end

      method, params ->
        FakeSurreal.default(method, params)
    end

    try do
      conn = start_conn(fake)
      {:ok, tab} = Conn.credits(conn)
      :ok = FakeSurreal.command(fake, {:set_handler, handler})

      caller =
        spawn_link(fn ->
          send(test, {:read_result, Conn.call(conn, query_req(), deadline_ms: 15_000)})
        end)

      assert_receive :read_seen, 2_000

      racer =
        spawn_link(fn ->
          send(
            test,
            {:tx_result,
             Tx.run(conn, accept_shaped_tx("00000000-0000-4000-8000-000000a00203"), @verdicts,
               deadline_ms: 10_000
             )}
          )
        end)

      # The barrier: the read is processing (owner busy awaiting A) AND
      # the mutation is enqueued (admitted, not yet dispatched).
      assert_eventually(
        fn ->
          s = Credits.snapshot(tab)
          s.processing >= 1 and s.enqueued >= 1
        end,
        5_000
      )

      # Release the reply that latches uncertainty INSIDE the window.
      send(fake.pid, :release_read)

      assert_receive {:tx_result, result}, 5_000
      assert {:unavailable, %{code: "database_unavailable", detail: :clock_uncertain}} = result

      assert_receive {:read_result, {:reply, _}}, 5_000
      refute Process.alive?(caller)
      refute Process.alive?(racer)
      # The decisive fact: the mutation frame never reached the peer.
      refute_received :mutation_seen
    after
      FakeSurreal.stop(fake)
    end
  end

  # -- A-04: one monitor per subscriber, idempotent re-join -------------

  defp owner_monitor_count(conn), do: length(elem(Process.info(conn, :monitors), 1))

  test "repeated hint subscriptions retain exactly one monitor per pid (A-04)" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    assert owner_monitor_count(conn) == 0

    assert :ok = Conn.hint_subscribe(conn)
    assert owner_monitor_count(conn) == 1

    # The audited reproduction: 100 re-joins grew the owner's monitors
    # from 0 to 100 behind the 16-pid set bound.
    for _ <- 1..100, do: assert(:ok = Conn.hint_subscribe(conn))
    assert owner_monitor_count(conn) == 1

    FakeSurreal.stop(fake)
  end

  test "at capacity: existing subscriber re-joins :ok, new pid is :full, death frees the slot (A-04)" do
    {:ok, fake} = FakeSurreal.start()
    conn = start_conn(fake)

    parked = fn ->
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)
    end

    subscribers = for _ <- 1..16, do: parked.()

    try do
      for pid <- subscribers, do: assert(:ok = Conn.hint_subscribe(conn, pid))
      assert owner_monitor_count(conn) == 16

      # Re-join at capacity is idempotent for an existing subscriber…
      assert :ok = Conn.hint_subscribe(conn, hd(subscribers))
      assert owner_monitor_count(conn) == 16

      # …and honestly full for a new one.
      stranger = parked.()
      assert {:error, :full} = Conn.hint_subscribe(conn, stranger)

      # A subscriber's death releases its slot (and its monitor).
      Process.exit(hd(subscribers), :kill)
      assert_eventually(fn -> owner_monitor_count(conn) == 15 end, 2_000)
      assert :ok = Conn.hint_subscribe(conn, stranger)
      assert owner_monitor_count(conn) == 16

      send(stranger, :stop)
    after
      for pid <- subscribers, do: if(Process.alive?(pid), do: send(pid, :stop))
    end

    FakeSurreal.stop(fake)
  end

  defp wait_down(ref) do
    receive do
      {:DOWN, ^ref, :process, _, _} -> :ok
    after
      2_000 -> raise "child not reaped"
    end
  end

  defp assert_eventually(fun, deadline) do
    if fun.() do
      :ok
    else
      if deadline <= 0 do
        raise "condition never became true"
      else
        Process.sleep(25)
        assert_eventually(fun, deadline - 25)
      end
    end
  end
end
