defmodule DrinkMeAlice.LiveTest do
  @moduledoc """
  Real-server evidence (pinned SurrealDB 3.0.5, private loopback child)
  for the extracted connector: Session, Admin provisioning, the generic
  migration runner, Tx outcome classification and Graph edges.
  """
  use ExUnit.Case, async: false

  @moduletag :live

  alias DrinkMeAlice.{Admin, Conn, Graph, Migrations, Session, Tx}
  alias DrinkMeAlice.Test.Live
  alias DrinkMeAlice.TestServer

  setup_all do
    live = Live.start!(ns: "lt", db: "lt")
    conn = Live.start_conn!(live)
    on_exit(fn -> Live.stop!(live) end)
    %{srv: live, conn: conn}
  end

  test "a second start on the fixture's data dir is refused, naming the claim holder", %{
    srv: live
  } do
    dir = Path.expand(live.dir)
    assert {:error, {:data_dir_in_use, ^dir, holder}} = TestServer.start(dir: live.dir)
    # The claim is held by the fixture start's dedicated holder process
    # (monitored to the owner), not by the owner itself.
    assert holder == live.server.claim_pid
    assert is_pid(holder)
  end

  @m1 %{
    version: 1,
    name: "notes",
    statements: [
      "DEFINE TABLE note SCHEMAFULL",
      "DEFINE FIELD body ON TABLE note TYPE string",
      "DEFINE FIELD n ON TABLE note TYPE int",
      "DEFINE INDEX note_body_uq ON TABLE note FIELDS body UNIQUE"
    ]
  }

  @m2 %{
    version: 2,
    name: "quota",
    statements: [
      "DEFINE TABLE quota SCHEMAFULL",
      "DEFINE FIELD used ON TABLE quota TYPE int",
      "DEFINE FIELD cap ON TABLE quota TYPE int"
    ],
    seed: ["UPSERT ONLY quota:main SET used = used ?? 0, cap = cap ?? $cap"],
    vars: &__MODULE__.quota_vars/1
  }

  def quota_vars(opts), do: {:ok, %{"cap" => Keyword.get(opts, :cap, 3)}}

  describe "Session + Admin" do
    test "root session runs queries and reports per-statement results", %{srv: live} do
      {:ok, s} = Session.open(live.root)
      assert {:ok, s, [%{"status" => "OK", "result" => 2}]} = Session.query(s, "RETURN 1 + 1;")

      assert {:error, {:rpc_error, %{"code" => _}}} =
               Session.query(s, "RETURN type::thing('a', 'b');")

      Session.close(s)
      Session.close(s)
    end

    test "wrong credentials are refused and leave no session", %{srv: live} do
      assert {:error, {:rpc_error, _code}} = Session.open(%{live.root | pass: "not-the-pass"})
    end

    test "provision validates everything it splices into DDL", %{srv: live} do
      assert {:error, {:invalid_option, :runtime_pass}} =
               Admin.provision(live.root, "rt_x", "")

      assert {:error, {:invalid_option, :runtime_pass}} =
               Admin.provision(live.root, "rt_x", "abcdefghijklmnop'; REMOVE TABLE x; --")

      assert {:error, {:invalid_option, :runtime_user}} =
               Admin.provision(live.root, "rt x", "abcdefghijklmnop0123")

      assert {:error, {:invalid_option, :token_dur}} =
               Admin.provision(live.root, "rt_x", "abcdefghijklmnop0123", token_dur: "1h; DROP")
    end

    test "the runtime identity is database-scoped: it cannot sign in as root", %{srv: live} do
      assert {:error, _} = Session.open(Map.put(live.runtime, :signin, :root))
      assert {:ok, s} = Session.open(Map.put(live.runtime, :signin, :database))
      Session.close(s)
    end
  end

  describe "Migrations" do
    test "apply, re-apply, seed, checksum drift, lock", %{srv: live} do
      # Other describe blocks may have migrated first (random order), so the
      # seeded cap is whatever the FIRST apply chose; later applies must not
      # re-seed it.
      assert :ok = Admin.migrate(live.root, [@m1, @m2], cap: 5)
      {:ok, s} = Session.open(live.root)

      {:ok, s, [%{"result" => %{"cap" => cap}}]} =
        Session.query(s, "SELECT cap FROM ONLY quota:main;")

      assert cap in [3, 5]

      # Idempotent: applied versions are skipped, seeds are not rerun.
      assert :ok = Admin.migrate(live.root, [@m1, @m2], cap: 99)
      {:ok, s, [%{"result" => q}]} = Session.query(s, "SELECT cap FROM ONLY quota:main;")
      assert q == %{"cap" => cap}

      {:ok, s, [%{"result" => rows}]} =
        Session.query(
          s,
          "SELECT version, state, checksum FROM schema_migration ORDER BY version;"
        )

      assert [%{"version" => 1, "state" => "applied"}, %{"version" => 2, "state" => "applied"}] =
               rows |> Enum.take(2) |> Enum.map(&Map.take(&1, ["version", "state"]))

      assert hd(rows)["checksum"] == Migrations.checksum(@m1.statements)

      # An edited, already-applied migration is refused, never re-run.
      edited = %{
        @m1
        | statements: @m1.statements ++ ["DEFINE FIELD extra ON TABLE note TYPE option<string>"]
      }

      assert {:error, {:dirty, 1, {:checksum_mismatch, _}}} =
               Admin.migrate(live.root, [edited, @m2])

      # A lock held by another owner is a hard conflict. The DELETE runs
      # even when the assertion fails (as on unqualified servers), so a
      # conservative-unknown outcome here cannot cascade into later
      # setups through a dirty lock row.
      try do
        {:ok, _, _} =
          Session.query(
            s,
            "UPSERT ONLY migration_lock:main SET owner = 'someone-else', acquired_at = time::now();"
          )

        assert {:error, :locked} = Admin.migrate(live.root, [@m1, @m2])
      after
        {:ok, _, _} = Session.query(s, "DELETE migration_lock:main;")
      end

      Session.close(s)
    end

    test "a failing migration rolls back whole; a corrected one then applies", %{srv: live} do
      {:ok, s} = Session.open(%{live.root | db: "mig_fail"})
      {:ok, s, _} = Session.query(s, "DEFINE DATABASE IF NOT EXISTS mig_fail;")
      Session.close(s)

      root = %{live.root | db: "mig_fail"}

      bad = %{
        version: 1,
        name: "bad",
        statements: ["DEFINE TABLE ok_t SCHEMAFULL", "THROW 'boom'"]
      }

      assert {:error, {:statement_err, _}} = Admin.migrate(root, [bad])

      # The whole transaction rolled back: no table, and the version is not applied.
      {:ok, s} = Session.open(root)
      {:ok, s, [%{"result" => info}]} = Session.query(s, "INFO FOR DB;")
      refute Map.has_key?(info["tables"] || %{}, "ok_t")

      # The `started` mark rolled back with it — the version is still
      # pending, so the corrected migration applies.
      {:ok, s, [%{"result" => []}]} = Session.query(s, "SELECT * FROM schema_migration;")
      Session.close(s)

      good = %{bad | statements: ["DEFINE TABLE ok_t SCHEMAFULL"]}
      assert :ok = Admin.migrate(root, [good])
    end

    test "migration lists are validated before any socket opens" do
      # Port 1 is unreachable: a list error must surface first.
      unreachable = %{host: "127.0.0.1", port: 1, ns: "x", db: "x", user: "u", pass: "p"}

      assert {:error, {:invalid_migrations, :versions_not_contiguous}} =
               Admin.migrate(unreachable, [%{@m1 | version: 2}])

      assert {:error, {:invalid_migrations, {:name, 1}}} =
               Admin.migrate(unreachable, [%{@m1 | name: "bad name"}])

      assert {:error, {:invalid_migrations, :empty}} = Admin.migrate(unreachable, [])
    end
  end

  describe "Tx through Conn (database-scoped runtime user)" do
    setup %{srv: live} do
      :ok = Admin.migrate(live.root, [@m1, @m2])
      :ok
    end

    defp note_tx(body) do
      {:ok, tx} =
        Tx.build(
          [
            "BEGIN TRANSACTION",
            "LET $q = (SELECT used, cap FROM ONLY quota:main)",
            "IF $q.used >= $q.cap { THROW \"bs:quota_full\" }",
            "CREATE note SET body = <string>$body, n = 1",
            "UPDATE quota:main SET used += 1",
            "COMMIT TRANSACTION",
            "RETURN {decision: \"created\", used: (SELECT VALUE used FROM ONLY quota:main)}"
          ],
          %{body: body}
        )

      tx
    end

    test "commit, unique-index abort and THROW abort are definite outcomes", %{
      conn: conn,
      srv: live
    } do
      body = "note-" <> Integer.to_string(System.unique_integer([:positive]))
      verdicts = %{"created" => :committed}

      assert {:committed, %{"decision" => "created", "used" => used}} =
               Tx.run(conn, note_tx(body), verdicts)

      assert is_integer(used)

      # Same body → unique index violation inside the tx → qualified abort.
      assert {:rejected, %{code: "internal_error", detail: {:tx_aborted, msg}}} =
               Tx.run(conn, note_tx(body), verdicts)

      assert msg =~ "note_body_uq"

      # Fill the quota, then the THROW guard aborts — and nothing was charged.
      {:ok, s} = Session.open(live.root)
      {:ok, s, _} = Session.query(s, "UPDATE quota:main SET used = cap;")

      assert {:rejected, %{detail: {:tx_aborted, msg}}} =
               Tx.run(conn, note_tx(body <> "-x"), verdicts)

      assert msg =~ "bs:quota_full"

      {:ok, s, [%{"result" => [n]}]} =
        Session.query(s, "SELECT VALUE count() FROM note WHERE body = $b GROUP ALL;", %{
          "b" => body <> "-x"
        })

      assert n in [0, nil]
      {:ok, _s, _} = Session.query(s, "UPDATE quota:main SET used = 0;")
    end

    test "a UUID-shaped string survives as a string with the <string> cast", %{conn: conn} do
      uuid =
        "0b8e8c1a-1111-4000-8000-" <>
          String.slice(Base.encode16(:crypto.strong_rand_bytes(6), case: :lower), 0, 12)

      assert {:committed, _} = Tx.run(conn, note_tx(uuid), %{"created" => :committed})

      assert {:ok, [%{"status" => "OK", "result" => [row]}]} =
               DrinkMeAlice.query(
                 conn,
                 "SELECT body, type::is_string(body) AS s FROM note WHERE body = <string>$b;",
                 %{b: uuid}
               )

      assert row == %{"body" => uuid, "s" => true}
    end

    test "build refuses scripts the classifier could never judge" do
      assert {:error, {:bad_tx, :too_few_statements}} =
               Tx.build([
                 "BEGIN TRANSACTION",
                 "CREATE x",
                 "COMMIT TRANSACTION",
                 "RETURN {decision: \"x\"}"
               ])

      assert {:error, {:bad_tx, :commit_position}} =
               Tx.build([
                 "BEGIN TRANSACTION",
                 "LET $a = 1",
                 "COMMIT TRANSACTION",
                 "LET $b = 2",
                 "RETURN 1"
               ])

      assert {:error, {:bad_tx, :no_begin}} =
               Tx.build([
                 "LET $a = 1",
                 "LET $b = 1",
                 "LET $c = 1",
                 "COMMIT TRANSACTION",
                 "RETURN 1"
               ])
    end

    test "an oversized request is certified unsent (the adapter this came from crashed here)", %{
      conn: conn
    } do
      big = String.duplicate("a", 60_000)

      {:ok, tx} =
        Tx.build(
          [
            "BEGIN TRANSACTION",
            "LET $a = 1",
            "LET $b = 2",
            "COMMIT TRANSACTION",
            "RETURN {decision: \"x\"}"
          ],
          Map.new(1..20, &{"v#{&1}", big})
        )

      assert {:unavailable, %{code: "bounded_limit_exceeded"}} =
               Tx.run(conn, tx, %{"x" => :committed})
    end
  end

  describe "Graph" do
    setup %{srv: live} do
      m3 = %{
        version: 3,
        name: "graph",
        statements:
          ["DEFINE TABLE node SCHEMAFULL", "DEFINE FIELD label ON TABLE node TYPE string"] ++
            DrinkMeAlice.Ash.Schema.edge_statements("blocks", "node", "node", [
              {"why", "option<string>"}
            ])
      }

      :ok = Admin.migrate(live.root, [@m1, @m2, m3])

      {:ok, s} = Session.open(live.root)

      {:ok, s, _} =
        Session.query(
          s,
          "DELETE blocks; UPSERT node:a SET label = 'a'; UPSERT node:b SET label = 'b'; UPSERT node:c SET label = 'c';"
        )

      Session.close(s)
      :ok
    end

    test "relate, traverse both directions, unrelate", %{conn: conn} do
      assert {:committed, %{"edge" => %{"in" => "node:a", "out" => "node:b", "why" => "needs b"}}} =
               Graph.relate(conn, "blocks", {"node", "a"}, {"node", "b"}, %{why: "needs b"})

      assert {:committed, _} = Graph.relate(conn, "blocks", {"node", "a"}, {"node", "c"})
      assert {:ok, out} = Graph.outgoing(conn, {"node", "a"}, "blocks", "node")
      assert Enum.sort(out) == ["b", "c"]
      assert {:ok, ["a"]} = Graph.incoming(conn, {"node", "c"}, "blocks", "node")

      assert {:committed, _} = Graph.unrelate(conn, "blocks", {"node", "a"}, {"node", "c"})
      assert {:ok, ["b"]} = Graph.outgoing(conn, {"node", "a"}, "blocks", "node")
    end

    test "names are identifiers or refused", %{conn: conn} do
      assert {:error, {:not_identifier, _}} =
               Graph.relate(conn, "blocks;DELETE", {"node", "a"}, {"node", "b"})

      assert {:error, {:not_identifier, _}} =
               Graph.outgoing(conn, {"node x", "a"}, "blocks", "node")

      assert {:error, {:bad_field, "in"}} =
               Graph.relate(conn, "blocks", {"node", "a"}, {"node", "b"}, %{"in" => 1})
    end
  end

  describe "Conn against the real server" do
    test "reconnects after a server restart on the same port and keeps working", %{srv: live} do
      {:ok, pid} = Conn.start(live.runtime |> Map.to_list() |> Keyword.put(:poll_ms, 5))
      Live.await_healthy!(pid)
      assert {:ok, [%{"result" => 3}]} = DrinkMeAlice.query(pid, "RETURN 1 + 2;")
      :ok = Conn.inject(pid, :kill_socket)
      Live.await_healthy!(pid)
      assert {:ok, [%{"result" => 4}]} = DrinkMeAlice.query(pid, "RETURN 2 + 2;")
      assert Conn.stats(pid).reconnects >= 2
      GenServer.stop(pid)
    end

    test "invalid config is refused before a process starts" do
      assert {:error, {:invalid_config, :ns}} =
               Conn.start(host: "127.0.0.1", port: 1, ns: "bad ns", db: "d", user: "u", pass: "p")

      assert {:error, {:invalid_config, :live}} =
               Conn.start(
                 host: "127.0.0.1",
                 port: 1,
                 ns: "n",
                 db: "d",
                 user: "u",
                 pass: "p",
                 live: "x;y"
               )
    end
  end

  describe "Fixture lifecycle (A-07)" do
    test "stop! stops the fixture conn first, proves cleanup, removes the directory" do
      live = Live.start!(ns: "lc", db: "lc")
      conn = Live.start_conn!(live)
      assert {:ok, [%{"result" => 1}]} = DrinkMeAlice.query(conn, "RETURN 1;")

      # A proven teardown returns :ok and removes the exclusive dir; an
      # unproven one raises and retains the directory instead.
      assert :ok = Live.stop!(live)
      refute File.dir?(live.dir)
    end

    test "the fixture owner outlives its parent, reaps the server, and a late stop! reads its receipt" do
      parent = self()

      holder =
        spawn(fn ->
          live = Live.start!(ns: "lc2", db: "lc2")
          send(parent, {:fixture, live})

          receive do
            :release -> :ok
          end
        end)

      assert_receive {:fixture, live}, 30_000
      os_pid = live.server.os_pid
      assert os_pid_alive?(os_pid)

      # The parent that started the fixture dies; the dedicated owner
      # (which holds the server port) must still tear the server down.
      Process.exit(holder, :kill)
      assert_eventually(fn -> not os_pid_alive?(os_pid) end, 20_000)

      # The owner has now fully exited; the late PUBLIC cleanup call
      # resolves through the receipt it left — a proven teardown reads
      # as :ok, an unproven one would raise and retain the evidence.
      owner_pid = live.owner
      owner_ref = Process.monitor(owner_pid)

      assert_receive {:DOWN, ^owner_ref, :process, ^owner_pid, _reason}, 10_000
      assert :ok = Live.stop!(live)
      refute File.dir?(live.dir)
    end
  end

  defp os_pid_alive?(os_pid) do
    case System.cmd("kill", ["-0", Integer.to_string(os_pid)], stderr_to_stdout: true) do
      {_out, 0} -> true
      _ -> false
    end
  end

  defp assert_eventually(fun, deadline) do
    if fun.() do
      :ok
    else
      if deadline <= 0 do
        raise "condition never became true"
      else
        Process.sleep(50)
        assert_eventually(fun, deadline - 50)
      end
    end
  end
end
