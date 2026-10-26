defmodule DrinkMeAlice.Test.Live do
  @moduledoc """
  One private pinned-SurrealDB server per test module: root config for
  `Admin`/`Session`, a provisioned database-scoped runtime user, and a
  supervised `Conn` signed in as that user (least privilege — the same
  split: root only for setup).

  ## Ownership and cleanup (A-07)

  The server port is owned by a dedicated owner process that stays alive
  through teardown — never by the `setup_all` process, which is gone
  before `on_exit` runs; a port whose owner died cannot deliver its exit
  status, so cleanup could never be proven. `stop!/1`:

  - stops every registered connection owner FIRST (an open connection
    can hold the server's graceful shutdown past its TERM grace);
  - stops the server IN the owner process, where the port lives, so
    TERM/exit-status observation actually observes;
  - removes the data directory only when cleanup is PROVEN
    (`{:exit_status, _}` or reaped-without-status);
  - raises — failing the test run — and RETAINS the directory as
    evidence when cleanup is unproven.

  The fixture root is claimed exclusively with a collision-resistant
  name (wall clock + VM counter + strong random), so independent BEAM
  runs cannot select or clobber each other's directories. If the parent
  dies before `stop!/1`, the owner still tears the server down.
  """

  alias DrinkMeAlice.{Admin, Conn, TestServer}

  def start!(opts \\ []) do
    {:ok, dir} = claim_dir()
    parent = self()

    owner =
      spawn(fn ->
        parent_ref = Process.monitor(parent)

        case TestServer.start(dir: dir) do
          {:ok, server} ->
            send(parent, {:server_started, self(), server})
            owner_loop(parent, parent_ref, dir, server, [])

          {:error, reason} ->
            send(parent, {:server_failed, self(), reason})
        end
      end)

    owner_ref = Process.monitor(owner)

    server =
      receive do
        {:server_started, ^owner, server} ->
          Process.demonitor(owner_ref, [:flush])
          server

        {:server_failed, ^owner, reason} ->
          Process.demonitor(owner_ref, [:flush])
          raise "live fixture server failed to start: #{inspect(reason)}"

        {:DOWN, ^owner_ref, :process, ^owner, reason} ->
          raise "live fixture owner died during startup: #{inspect(reason)}"
      after
        30_000 -> raise "live fixture server did not start"
      end

    root = %{
      host: "127.0.0.1",
      port: server.http_port,
      ns: Keyword.get(opts, :ns, "bs"),
      db: Keyword.get(opts, :db, "bs"),
      user: server.creds.user,
      pass: server.creds.pass
    }

    runtime_user = "rt_" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
    runtime_pass = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
    :ok = Admin.provision(root, runtime_user, runtime_pass)

    runtime = %{root | user: runtime_user, pass: runtime_pass}
    %{owner: owner, server: server, dir: dir, root: root, runtime: runtime}
  end

  def start_conn!(live, extra \\ []) do
    config = live.runtime |> Map.to_list() |> Keyword.merge(poll_ms: 5) |> Keyword.merge(extra)
    {:ok, pid} = Conn.start(config)
    await_healthy!(pid)
    # The owner stops registered conns BEFORE the server at teardown.
    send(live.owner, {:register_conn, pid})
    pid
  end

  def await_healthy!(pid, budget_ms \\ 5_000) do
    cond do
      Conn.status(pid) == :healthy ->
        :ok

      budget_ms <= 0 ->
        raise "conn not healthy: #{inspect(Conn.stats(pid).last_error)}"

      true ->
        Process.sleep(20)
        await_healthy!(pid, budget_ms - 20)
    end
  end

  def stop!(live) do
    owner = live.owner
    ref = make_ref()

    # The owner may already have exited (its parent — the setup process —
    # died, or a racing teardown completed). Every already-exited path
    # resolves through the receipt the owner left behind; a receipt is
    # deleted ONLY after a proven outcome, so unproven cleanup keeps its
    # evidence (R-04).
    result =
      if Process.alive?(owner) do
        mref = Process.monitor(owner)
        send(owner, {:stop, self(), ref})

        receive do
          {^ref, reply} ->
            Process.demonitor(mref, [:flush])
            {:replied, reply}

          {:DOWN, ^mref, :process, ^owner, reason} when reason in [:normal, :noproc] ->
            # Exited after (or racing) the monitor: its teardown result
            # is in the receipt — read it, never assume it was clean.
            {:from_receipt}

          {:DOWN, ^mref, :process, ^owner, reason} ->
            {:error, {:owner_died, reason}}
        after
          30_000 -> {:error, :owner_unresponsive}
        end
      else
        {:from_receipt}
      end

    outcome =
      case result do
        {:replied, reply} -> reply
        {:from_receipt} -> read_receipt(live.dir)
        {:error, _} = err -> err
      end

    case outcome do
      {:ok, _proven} ->
        File.rm_rf(receipt_path(live.dir))
        File.rm_rf(live.dir)
        :ok

      other ->
        # Cleanup is unproven: retain the directory AND the receipt as
        # evidence and fail the run — never report a clean teardown that
        # did not happen (A-07/R-04).
        raise """
        live fixture cleanup unproven; directory RETAINED for evidence
          dir: #{live.dir}
          result: #{inspect(other)}
        """
    end
  end

  # -- owner ------------------------------------------------------------------

  defp owner_loop(parent, parent_ref, dir, server, conns) do
    receive do
      {:register_conn, pid} ->
        owner_loop(parent, parent_ref, dir, server, [pid | List.delete(conns, pid)])

      {:stop, from, ref} ->
        send(from, {ref, teardown(server, dir, conns)})

      {:DOWN, ^parent_ref, :process, ^parent, _reason} ->
        # The parent is gone: still tear the server down, prove what we
        # can, and leave the receipt for a late stop!/1.
        _ = teardown(server, dir, conns)
        :ok
    end
  end

  defp teardown(server, dir, conns) do
    for pid <- conns, do: stop_conn(pid)
    reply = stop_server(server)

    case reply do
      {:ok, _proven} ->
        _ = File.rm_rf(dir)
        _ = File.write(receipt_path(dir), :erlang.term_to_binary(reply))
        reply

      {:error, _} = err ->
        # Unproven: RETAIN the directory; the receipt records why.
        _ = File.write(receipt_path(dir), :erlang.term_to_binary(err))
        err
    end
  end

  defp stop_server(server) do
    case TestServer.stop(server) do
      {:exit_status, _n} = proven -> {:ok, proven}
      {:reaped, :status_unobserved} = proven -> {:ok, proven}
      {:error, reason} -> {:error, reason}
    end
  end

  defp receipt_path(dir), do: dir <> ".teardown"

  defp read_receipt(dir) do
    case File.read(receipt_path(dir)) do
      {:ok, binary} ->
        try do
          :erlang.binary_to_term(binary)
        rescue
          _ -> {:error, {:unreadable_receipt, byte_size(binary)}}
        end

      {:error, _} ->
        # The owner exited normally WITHOUT a receipt — its teardown
        # result is unknown; treat the directory as unproven evidence.
        {:error, :no_teardown_receipt}
    end
  end

  defp stop_conn(pid) do
    if Process.alive?(pid) do
      try do
        GenServer.stop(pid, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end
    else
      :ok
    end
  end

  # Exclusive, collision-resistant claim: two BEAMs on one host (or one
  # BEAM across restarts) cannot select the same root.
  defp claim_dir do
    tmp = System.tmp_dir!()

    Enum.find_value(1..32, fn _ ->
      name =
        "drink_me_alice_" <>
          Integer.to_string(:os.system_time(:millisecond)) <>
          "_" <>
          Integer.to_string(System.unique_integer([:positive])) <>
          "_" <>
          Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)

      dir = Path.join(tmp, name)

      case File.mkdir(dir) do
        :ok -> {:ok, dir}
        {:error, :eexist} -> nil
        {:error, reason} -> {:error, {:dir_claim, reason}}
      end
    end) || {:error, :dir_claim_failed}
  end
end
