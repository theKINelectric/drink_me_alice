defmodule DrinkMeAlice.TestServer do
  @max_log_bytes 262_144
  @max_drain_items 512
  @health_deadline_ms 10_000
  @term_grace_ms 5_000
  @kill_grace_ms 3_000
  @qualified_version "3.0.5"
  @version_probe_timeout_ms 2_000
  @scan_timeout_ms 500
  @release_timeout_ms 2_000

  @moduledoc """
  Owned, private, loopback-only SurrealDB server child — for tests of
  this library and of anything built on it.

  - Data lives under a caller-provided private directory (`<dir>/data`,
    SurrealKV); the directory is created mode 0700.
  - Root credentials are freshly generated per server, passed only via the
    `SURREAL_USER`/`SURREAL_PASS` environment (never on the command line),
    and never written to logs or receipts.
  - Bound to an ephemeral loopback port picked by the kernel.
  - The server's full version identity is recorded as the first line of
    `<dir>/server.log` — a bare version number does not identify an
    artifact.
  - Port (stderr) output is appended to `<dir>/server.log` with a real
    cumulative byte cap (#{@max_log_bytes}); `server.log_bytes` tracks it.
    Long-running probes should call `drain/2` inside their wait loops so
    diagnostic bytes never accumulate in the port mailbox.
  - Startup reports definite failure on early child exit (including bind
    failure); a `/health` 200 counts only while the owned child is alive.
  - Concurrent starts on the SAME data dir are refused, never raced.
    Within this VM a per-dir claim — a dedicated holder process per
    start, monitoring the port owner, so one caller may hold several
    stores while a claim still dies with its owner — makes the second
    caller fail fast with `{:error, {:data_dir_in_use, dir,
    holder_pid}}`. Against an already-existing store, the SurrealKV
    endpoint is scanned for a SUSPECTED foreign `surreal start`
    (typically a leftover from a killed VM) and refused with
    `{:error, {:data_dir_lock_suspected, dir, suspects}}` BEFORE any
    child is spawned or the old log is touched. The scan is
    best-effort, fail-open, and suspicion is a hint to verify — not
    verified lock ownership; an early exit that still slips through
    its race window is reported with the log tail and the suspected
    foreign holders.
  - `stop/1` escalates TERM -> KILL under finite deadlines and confirms the
    OS child is reaped; the last observed exit status is preserved, and
    the data-dir claim is released — only for THIS instance's claim,
    only on proven cleanup, and SYNCHRONOUSLY: normally `stop/1` does
    not report completion before the registration is removed, so an
    immediate same-dir restart is not refused by our own stale claim.
    If the bounded release handshake times out (a holder that cannot
    run), the result preserves both facts as
    `{:error, {:claim_release_unconfirmed, holder, cleanup}}`. A
    stale handle or an unproven cleanup cannot strip a live claim; a
    FAILED start with unproven child cleanup (`{:acquisition_unclean,
    reason, holder}` or `{:cleanup_unproven, other, holder}`) retains
    its claim — with the holder pid in the error as a receipt — until
    the starting process dies.

  ## Version qualification

  The suite is written and qualified against SurrealDB
  #{@qualified_version} ONLY. `start/1` resolves the executable
  (`SURREAL_BIN`, else `surreal` on PATH), records its identity and
  refuses a different version with `{:error,
  {:unqualified_server, reason}}` — a later 3.x build is not
  automatically the qualified behavior. Set `SURREAL_ALLOW_UNQUALIFIED=1`
  to start anyway: that is a compatibility EXPERIMENT, not a
  qualification, and every such run says so on stdout.
  """

  defstruct [
    :port,
    :os_pid,
    :http_port,
    :dir,
    :log_path,
    :creds,
    :exit_status,
    :claim_pid,
    log_bytes: 0
  ]

  @type creds :: %{user: String.t(), pass: String.t()}

  @type t :: %__MODULE__{
          port: port(),
          os_pid: non_neg_integer(),
          http_port: :inet.port_number(),
          dir: String.t(),
          log_path: String.t(),
          creds: creds(),
          exit_status: integer() | nil,
          claim_pid: pid() | nil,
          log_bytes: non_neg_integer()
        }

  @doc "The SurrealDB version this suite is qualified against."
  @spec qualified_version() :: String.t()
  def qualified_version, do: @qualified_version

  @doc """
  `{bin, identity}` for the SurrealDB executable: `SURREAL_BIN` if set,
  else `surreal` on `PATH`. `identity` is the FULL `surreal version`
  output — build metadata included, because a version number alone does
  not identify the artifact under test.

  The version probe is bounded (#{@version_probe_timeout_ms} ms) and
  typed: a missing binary, an unusable path, a nonzero exit or a hung
  command is `{:error, reason}` — never a raise, never a parsed
  substring of failed output.
  """
  @spec resolve_bin() :: {:ok, binary(), binary()} | {:error, term()}
  def resolve_bin do
    bin = System.get_env("SURREAL_BIN") || System.find_executable("surreal")

    if bin do
      # The crash is trapped INSIDE the task so an unusable path is a
      # typed result, not a task-crash error report.
      probe =
        Task.async(fn ->
          try do
            System.cmd(bin, ["version"], stderr_to_stdout: true)
          rescue
            _ -> :crashed
          catch
            _, _ -> :crashed
          end
        end)

      case Task.yield(probe, @version_probe_timeout_ms) || Task.shutdown(probe, :brutal_kill) do
        {:ok, {out, 0}} -> {:ok, bin, String.trim(out)}
        {:ok, :crashed} -> {:error, :version_command_crashed}
        {:ok, {_out, exit}} -> {:error, {:version_command_failed, exit}}
        {:exit, _reason} -> {:error, :version_command_crashed}
        nil -> {:error, :version_command_timeout}
      end
    else
      {:error, :no_surreal_bin}
    end
  end

  @doc "The first `major.minor.patch` in a version line, or `:error`."
  @spec parse_version(binary()) :: {:ok, String.t()} | :error
  def parse_version(line) when is_binary(line) do
    case Regex.run(~r/(\d+\.\d+\.\d+)/, line) do
      [_, v] -> {:ok, v}
      nil -> :error
    end
  end

  @doc """
  Whether `:live` tests may run: `:ok`, `{:skip, reason}`, or — with
  `SURREAL_REQUIRE_QUALIFIED=1` set (CI) — `{:error, reason}` for
  anything that would otherwise be skipped, so a qualification job fails
  instead of silently excluding its live suite. A different installed
  server version skips with the reason; set
  `SURREAL_ALLOW_UNQUALIFIED=1` to run it anyway as a compatibility
  experiment whose results are NOT a qualification.
  """
  @spec live_policy() :: :ok | {:skip, String.t()} | {:error, String.t()}
  def live_policy do
    case server_policy() do
      :ok ->
        :ok

      {:not_live, reason} ->
        if System.get_env("SURREAL_REQUIRE_QUALIFIED") == "1",
          do: {:error, reason},
          else: {:skip, reason}
    end
  end

  defp server_policy do
    case resolve_bin() do
      {:error, reason} ->
        {:not_live, "no usable SurrealDB binary: #{inspect(reason)}"}

      {:ok, _bin, identity} ->
        case qualification(identity) do
          :ok -> :ok
          {:unqualified, reason} -> {:not_live, reason}
        end
    end
  end

  defp qualification(identity) do
    case parse_version(identity) do
      {:ok, v} when v == @qualified_version ->
        :ok

      {:ok, v} ->
        if unqualified_opted_in?() do
          IO.puts(
            "[drink_me_alice] SURREAL_ALLOW_UNQUALIFIED=1: running against #{v} " <>
              "(identity: #{identity}) — a compatibility experiment, NOT a qualification"
          )

          :ok
        else
          {:unqualified,
           "installed SurrealDB is #{v}, qualified is #{@qualified_version} " <>
             "(identity: #{identity}); set SURREAL_ALLOW_UNQUALIFIED=1 for a compatibility experiment"}
        end

      :error ->
        if unqualified_opted_in?() do
          IO.puts(
            "[drink_me_alice] SURREAL_ALLOW_UNQUALIFIED=1: version unreadable " <>
              "(#{inspect(identity)}) — a compatibility experiment, NOT a qualification"
          )

          :ok
        else
          {:unqualified, "unreadable SurrealDB version output: #{identity}"}
        end
    end
  end

  defp unqualified_opted_in?, do: System.get_env("SURREAL_ALLOW_UNQUALIFIED") == "1"

  @doc """
  The SurrealDB binary path, raising when absent (deprecated raise
  behavior kept for direct callers; prefer `resolve_bin/0`).
  """
  def surreal_bin do
    case resolve_bin() do
      {:ok, bin, _identity} ->
        bin

      {:error, reason} ->
        raise "no usable SurrealDB binary (#{inspect(reason)}): set SURREAL_BIN or put `surreal` on PATH"
    end
  end

  @doc """
  Start a private server. Options:

    * `:dir` (required) — private root; `<dir>/data` is the SurrealKV path,
      `<dir>/server.log` records the server identity and bounded stderr.
    * `:port` — bind this port instead of a free one (restart on the
      same endpoint, so a conn configured once reconnects to it).
    * `:user` / `:pass` — reuse these root credentials instead of
      generating fresh ones. REQUIRED when restarting onto an existing
      SurrealKV store: the stored root user persists, so fresh creds
      would not take effect and every signin would fail -32002.
    * `:args` — extra `surreal start` CLI args (e.g. `--allow-net`,
      `--async-event-interval`); must never contain credentials.
    * `:env` — extra env pairs (charlist keys/values).
    * `:health` — `false` to skip the /health wait.

  A caller may hold several servers at once (each start claims its own
  directory through a dedicated holder process). Refuses with
  `{:error, {:unqualified_server, reason}}` before creating anything
  when the resolved executable is not the qualified version and
  `SURREAL_ALLOW_UNQUALIFIED=1` is not set. Refuses with
  `{:error, {:data_dir_in_use, dir, holder_pid}}` when another live
  claim in this VM already owns `dir`. When the store already exists,
  a best-effort host process scan runs: a SUSPECTED foreign holder (a
  process whose executable is named `surreal` and whose arguments
  carry the exact endpoint token) refuses with
  `{:error, {:data_dir_lock_suspected, dir, suspects}}` before any
  child is spawned. A command-line match is a hint, not verified lock
  ownership: identify the process, confirm it really owns this store,
  and stop that owner — do not kill on a command-line match alone, and
  never delete a store that may still be active. The scan is
  fail-open. Directories are claimed by their expanded path. On error,
  pre-spawn failures release the claim synchronously (if that release
  cannot be confirmed, the result says so via
  `{:claim_release_unconfirmed, holder, reason}`); a start whose child
  was acquired but not proven cleaned up retains the claim — with the
  holder pid in the error as a receipt — until the starting process
  dies. Diagnostics are best-effort (a broken IO device cannot prevent
  finalization), and EVERY exception after acquisition — including one
  raised by failing diagnostics inside a rescue handler — is converted
  to proven cleanup or a retained claim with a typed receipt
  (`{:startup_failed, {:finalization_crashed, cause}, ...}`).
  """
  @spec start(keyword()) :: {:ok, t()} | {:error, term()}
  def start(opts) do
    dir = opts |> Keyword.fetch!(:dir) |> Path.expand()

    case claim_dir(dir) do
      {:ok, holder} ->
        try do
          start_claimed(opts, dir, holder)
        rescue
          e ->
            release_holder(holder)
            reraise e, __STACKTRACE__
        catch
          kind, reason ->
            release_holder(holder)
            :erlang.raise(kind, reason, __STACKTRACE__)
        else
          {:ok, _server} = ok ->
            ok

          {:error, reason} ->
            # What the claim owes for this failure: pre-spawn failures
            # release synchronously — and if that release cannot be
            # confirmed, the PUBLIC result says so instead of hiding a
            # pending release behind the original reason;
            # {:acquisition_unclean, _, _} (acquire wrapped it where the
            # port-exists state is known) RETAINS the exclusion with the
            # receipt until the starting process dies;
            # {:startup_failed, _, _} was already finalized by stop/1
            # under the identity+proof gate.
            case claim_disposition(reason) do
              :release ->
                case release_holder(holder) do
                  :ok ->
                    {:error, reason}

                  {:error, {:release_unconfirmed, _holder}} ->
                    {:error, {:claim_release_unconfirmed, holder, reason}}
                end

              _ ->
                {:error, reason}
            end
        end

      {:error, _reason} = refused ->
        refused
    end
  end

  # Public for disposition regressions only. {:acquisition_unclean, _, _}
  # is attached by acquire/8 where the port-exists state is directly
  # known; {:startup_failed, _, _} was finalized by stop/1 under the
  # identity+proof gate (its cleanup_unproven / claim_release_unconfirmed
  # interiors already carry receipts). Everything else failed before any
  # child existed.
  @doc false
  @spec claim_disposition(term()) :: :release | :retain | :decided
  def claim_disposition({:acquisition_unclean, _, _}), do: :retain
  def claim_disposition({:startup_failed, _, _}), do: :decided
  def claim_disposition(_), do: :release

  # One dedicated holder process per start. A :global name cannot be
  # registered by a PID that already holds one, so claiming from the
  # caller would forbid one caller from holding two different stores;
  # the holder also gives every start a distinct claim identity that
  # stop/1 must match before releasing. The holder monitors the
  # calling process — the port owner — so an owner that dies without
  # stopping releases its claim; that does NOT prove the OS child died
  # (measured: a spawned child can outlive its owning process), it
  # only clears the way for the suspected-holder scan on the next
  # start against the same store.
  defp claim_dir(dir, attempts \\ 5)

  defp claim_dir(_dir, 0), do: {:error, :claim_registration_unstable}

  defp claim_dir(dir, attempts) do
    key = claim_key(dir)
    parent = self()

    holder =
      spawn(fn ->
        case :global.register_name(key, self()) do
          :yes ->
            send(parent, {:claim_taken, self()})
            parent_ref = Process.monitor(parent)

            receive do
              {:DOWN, ^parent_ref, :process, ^parent, _reason} ->
                :ok

              {:release, reply_to, ref} ->
                # Removal happens BEFORE the ack, so the handshake's
                # completion proves the registration is gone.
                :global.unregister_name(key)
                send(reply_to, {:claim_released, ref})
                :ok
            end

          :no ->
            # A registration failure names a holder only when one is
            # still registered; a holder that died racing this attempt
            # is not evidence that the directory is in use.
            send(parent, {:claim_denied, :global.whereis_name(key)})
        end
      end)

    receive do
      {:claim_taken, ^holder} ->
        {:ok, holder}

      {:claim_denied, current} when is_pid(current) ->
        {:error, {:data_dir_in_use, dir, current}}

      {:claim_denied, :undefined} ->
        claim_dir(dir, attempts - 1)
    after
      5_000 ->
        if Process.alive?(holder), do: Process.exit(holder, :kill)
        {:error, {:claim_timeout, dir}}
    end
  end

  # Bounded synchronous release (W-01): a fire-and-forget message let a
  # PROVEN stop return while the claim was still registered, so an
  # immediate same-dir restart could be refused. Completion now means
  # the holder removed the registration and said so. A holder that
  # never answers (adversarially suspended) bounds the wait instead of
  # hanging; the diagnostic names the holder, and the claim clears when
  # it next runs or its owner dies.
  defp release_holder(holder) when is_pid(holder) do
    ref = make_ref()
    send(holder, {:release, self(), ref})

    receive do
      {:claim_released, ^ref} -> :ok
    after
      @release_timeout_ms ->
        safe_puts(
          "[drink_me_alice] TestServer claim release not confirmed within #{@release_timeout_ms} ms " <>
            "(holder #{inspect(holder)}); the claim clears when the holder runs again or its owner dies"
        )

        {:error, {:release_unconfirmed, holder}}
    end
  end

  # Key under which a data dir is claimed for this VM's lifetime of the
  # owning process. Public only so tests can hold and inspect the claim.
  @doc false
  @spec claim_key(String.t()) :: term()
  def claim_key(dir) when is_binary(dir), do: {__MODULE__, :data_dir, dir}

  defp start_claimed(opts, dir, holder) do
    with {:ok, bin, identity} <- resolve_bin(),
         :ok <- enforce_qualified(identity) do
      # The /health probe uses :httpc.
      {:ok, _} = Application.ensure_all_started(:inets)
      File.mkdir_p!(dir)
      File.chmod!(dir, 0o700)

      # Before spawning (or truncating the previous run's log): an
      # existing store may still be LOCKed by a leftover process from a
      # dead VM — refuse on SUSPICION instead of racing into
      # {:early_exit, 1}. The scan is best-effort and fail-open, and a
      # match is a hint to verify, never proof of lock ownership.
      case foreign_lock_holders(dir, nil) do
        [] ->
          log_path = Path.join(dir, "server.log")
          # Identity receipt: which executable this run actually tested.
          File.write!(log_path, "surreal identity: #{identity}\n")

          http_port = Keyword.get(opts, :port) || free_port()

          user =
            Keyword.get(opts, :user) ||
              "stq_" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)

          pass =
            Keyword.get(opts, :pass) ||
              Base.encode16(:crypto.strong_rand_bytes(24), case: :lower)

          args =
            [
              "start",
              "--no-banner",
              "--no-defaults",
              "--log",
              "error",
              "--bind",
              "127.0.0.1:#{http_port}"
            ] ++
              Keyword.get(opts, :args, []) ++
              ["surrealkv://#{dir}/data"]

          env =
            [
              {~c"SURREAL_USER", String.to_charlist(user)},
              {~c"SURREAL_PASS", String.to_charlist(pass)}
            ] ++ Keyword.get(opts, :env, [])

          case acquire(bin, {args, env}, http_port, dir, log_path, user, pass, holder) do
            {:ok, server} ->
              # The claim identity rides the server from the moment the
              # child exists, so startup finalization releases (or
              # retains) under the same identity+proof gate as stop/1.
              finalize_started(opts, %{server | claim_pid: holder})

            {:error, reason} ->
              {:error, reason}
          end

        holders ->
          {:error, {:data_dir_lock_suspected, dir, holders}}
      end
    end
  end

  defp enforce_qualified(identity) do
    case qualification(identity) do
      :ok -> :ok
      {:unqualified, reason} -> {:error, {:unqualified_server, reason}}
    end
  end

  # The OS pid surfaces asynchronously after fork — on slower hosts
  # (network mounts, darwin) a single immediate read races the child's
  # registration and reaps a HEALTHY server. Bounded retry; a real child
  # death still fails at the deadline (its exit status arrives on the
  # port).
  defp await_os_pid(port, deadline_ms) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} ->
        {:ok, pid}

      _ ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          {:error, :no_os_pid}
        else
          Process.sleep(10)
          await_os_pid(port, deadline_ms)
        end
    end
  end

  # The cleanup boundary from acquisition (W-02 round 2): EVERY
  # exception after the child exists — including one raised inside a
  # rescue handler by failing diagnostics — is converted here into
  # proven cleanup or a retained claim with a typed receipt. A raise
  # inside a rescue clause is not caught by that same handler, which is
  # exactly how a diagnostic IO failure once escaped to the outer
  # rescue, released the claim, and left the child running. With this
  # boundary, the outer rescue in start/1 is pre-spawn-only by
  # construction.
  defp finalize_started(opts, server) do
    try do
      try do
        if Keyword.get(opts, :health, true) do
          case await_health(server, System.monotonic_time(:millisecond) + @health_deadline_ms) do
            {:ok, server} -> {:ok, server}
            {:error, reason, server} -> startup_failed(server, reason)
          end
        else
          {:ok, server}
        end
      rescue
        e -> startup_failed(server, {:health_raised, Exception.message(e)})
      catch
        :exit, reason -> startup_failed(server, {:health_exit, reason})
        kind, reason -> startup_failed(server, {:health_catch, {kind, reason}})
      end
    rescue
      e -> finalization_crashed(server, crash_tag(e))
    catch
      :exit, reason -> finalization_crashed(server, {:exit, reason})
      kind, reason -> finalization_crashed(server, {kind, reason})
    end
  end

  # Rescued values are always exception structs; the module only (never
  # the message) — the secrets discipline.
  defp crash_tag(e) when is_exception(e), do: {:raise, e.__struct__}

  # An exception out of finalization itself: attempt the stop (a stop
  # that also crashes is unproven) and wrap under the same gate — the
  # result always preserves proven cleanup or a retained claim with the
  # holder receipt.
  defp finalization_crashed(server, cause) do
    stopped =
      try do
        stop(server)
      rescue
        _ -> {:error, :stop_crashed}
      catch
        _, _ -> {:error, :stop_crashed}
      end

    wrap_startup_result(server, {:finalization_crashed, cause}, stopped)
  end

  # Stop and reap the child; never mask an unproven cleanup result.
  # The cleanup_unproven shape carries the claim holder as a receipt:
  # the exclusion is RETAINED (the store may still be held) and clears
  # when the starting process dies — observable via
  # `:global.whereis_name(claim_key(dir))`.
  defp startup_failed(server, reason) do
    report_startup_evidence(server, reason)
    wrap_startup_result(server, reason, stop(server))
  end

  defp wrap_startup_result(server, reason, cleanup) do
    case cleanup do
      {:exit_status, _} ->
        {:error, {:startup_failed, reason, cleanup}}

      {:reaped, :status_unobserved} ->
        {:error, {:startup_failed, reason, cleanup}}

      # Child cleanup proven, release unconfirmed: propagate the
      # distinction, not just the original startup reason.
      {:error, {:claim_release_unconfirmed, holder, proven}} ->
        {:error, {:startup_failed, reason, {:claim_release_unconfirmed, holder, proven}}}

      other ->
        {:error, {:startup_failed, reason, {:cleanup_unproven, other, server.claim_pid}}}
    end
  end

  # Evidence only — no control flow depends on it, and BEST-EFFORT BY
  # CONSTRUCTION (W-02 round 2): a failing diagnostic — e.g. a group
  # leader that answers IO requests with an error — must never prevent
  # startup finalization, which once released the claim beside a
  # still-running child. A bounded log tail (credentials never reach
  # the log) plus any OTHER live process SUSPECTED of holding this
  # data path — a hint to verify, never proof of ownership, and never
  # an instruction to kill or delete anything.
  defp report_startup_evidence(server, reason) do
    try do
      server = drain(server, 200)

      safe_puts(
        "[drink_me_alice] TestServer startup failed (#{inspect(reason)}); " <>
          "log tail: #{inspect(log_tail(server.log_path))}; " <>
          "suspected foreign holders of this data path (verify before acting): " <>
          inspect(foreign_lock_holders(server.dir, server.os_pid))
      )
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  # Diagnostics must never break the machinery that reports them.
  defp safe_puts(message) do
    try do
      IO.puts(message)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  defp log_tail(log_path) do
    case File.read(log_path) do
      {:ok, content} ->
        content
        |> String.split("\n", trim: true)
        |> Enum.drop(1)
        |> Enum.join(" | ")
        |> then(&Regex.replace(~r/\e\[[0-9;]*[mA-Z]/, &1, ""))
        |> String.slice(-400, 400)

      {:error, _} ->
        "(log unreadable)"
    end
  end

  # Live `surreal start` processes (any host user) whose command line
  # references this dir's SurrealKV endpoint. `exclude_os_pid` drops our
  # own child when the evidence is gathered after spawning. A missing or
  # failed `ps` means no findings — the scan is fail-open, never a
  # refusal on absence of evidence.
  defp foreign_lock_holders(dir, exclude_os_pid) do
    endpoint = "surrealkv://#{dir}/data"

    if File.exists?(Path.join(dir, "data")) do
      ps_snapshot()
      |> Enum.flat_map(fn line ->
        case Integer.parse(String.trim_leading(line)) do
          {pid, " " <> cmd} ->
            cmd = String.trim_leading(cmd)

            if pid != exclude_os_pid and suspected_holder?(cmd, endpoint),
              do: [{pid, String.slice(cmd, 0, 300)}],
              else: []

          _ ->
            []
        end
      end)
    else
      []
    end
  end

  # SUSPECTED, not verified: the executable must be named `surreal` AND
  # the endpoint must appear as an exact whitespace-delimited token of
  # the command line — a bare substring would fire on sibling stores
  # (`.../data_unrelated` contains `.../data`) and on any process that
  # merely mentions the path. Even a full match proves neither the true
  # executable identity nor actual lock ownership; ps output loses argv
  # boundaries, so an adversary can still counterfeit a token. Public
  # for matcher regressions only.
  @doc false
  @spec suspected_holder?(String.t(), String.t()) :: boolean()
  def suspected_holder?(cmdline, endpoint) when is_binary(cmdline) and is_binary(endpoint) do
    tokens = String.split(cmdline, " ", trim: true)
    tokens != [] and Path.basename(hd(tokens)) == "surreal" and endpoint in tokens
  end

  defp ps_snapshot do
    task =
      Task.async(fn ->
        try do
          System.cmd("ps", ["-eo", "pid=,command="], stderr_to_stdout: true)
        rescue
          _ -> :crashed
        catch
          _, _ -> :crashed
        end
      end)

    case Task.yield(task, @scan_timeout_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, {out, 0}} -> String.split(out, "\n", trim: true)
      _ -> []
    end
  end

  # Acquire the child. Every failure AFTER the port exists is wrapped
  # as {:acquisition_unclean, reason, holder} AT THIS SITE — where the
  # acquisition state is directly known — marking cleanup as unproven
  # (a port close proves nothing about the OS child) and carrying the
  # claim receipt. The wrapper is what start/1's disposition reads;
  # acquisition state is represented, not inferred from error names.
  defp acquire(bin, {args, env}, http_port, dir, log_path, user, pass, holder) do
    case spawn_child(bin, args, env) do
      {:error, _} = err ->
        err

      port when is_port(port) ->
        os_pid =
          try do
            # The OS pid surfaces asynchronously after fork — on slower
            # hosts (network mounts, darwin) a single immediate read
            # races the child's registration and reaps a HEALTHY server.
            # Bounded retry; a real child death still fails at the
            # deadline (its exit status arrives on the port).
            await_os_pid(port, System.monotonic_time(:millisecond) + 1_000)
          rescue
            e -> {:error, {:port_info, Exception.message(e)}}
          catch
            :exit, reason -> {:error, {:port_info, {:exit, reason}}}
            kind, reason -> {:error, {:port_info, {kind, reason}}}
          end

        case os_pid do
          {:ok, pid} ->
            {:ok,
             %__MODULE__{
               port: port,
               os_pid: pid,
               http_port: http_port,
               dir: dir,
               log_path: log_path,
               creds: %{user: user, pass: pass}
             }}

          {:error, reason} ->
            # Port exists but OS pid unknown — close the port; the child
            # (if spawned) gets SIGTERM via a bounded kill attempt only if
            # we can name it.
            safe_port_close(port)
            {:error, {:acquisition_unclean, reason, holder}}
        end
    end
  end

  defp spawn_child(bin, args, env) do
    try do
      Port.open({:spawn_executable, bin}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: args,
        env: env
      ])
    rescue
      e -> {:error, {:port_open, Exception.message(e)}}
    catch
      k, r -> {:error, {:port_open, {k, r}}}
    end
  end

  defp free_port do
    {:ok, l} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    {:ok, {_, n}} = :inet.sockname(l)
    :ok = :gen_tcp.close(l)
    n
  end

  # Health requires BOTH a 200 from /health AND the owned child still alive
  # with no observed exit status — a 200 alone cannot prove our child owns
  # the port.
  defp await_health(server, deadline) do
    case poll_once(server) do
      {:healthy, server} ->
        {:ok, server}

      {:dead, status, server} ->
        {:error, {:early_exit, status}, server}

      {:unhealthy, server} ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :health_deadline, server}
        else
          Process.sleep(50)
          await_health(server, deadline)
        end
    end
  end

  defp poll_once(server) do
    server = drain(server, 0)

    cond do
      server.exit_status != nil ->
        {:dead, server.exit_status, server}

      not alive?(server) ->
        {:dead, :proc_gone, server}

      true ->
        {:ok, _} = Application.ensure_all_started(:inets)

        result =
          :httpc.request(
            :get,
            {String.to_charlist("http://127.0.0.1:#{server.http_port}/health"), []},
            [timeout: 300, connect_timeout: 300],
            []
          )

        case result do
          {:ok, {{_, 200, _}, _, _}} -> {:healthy, server}
          _ -> {:unhealthy, server}
        end
    end
  end

  @doc """
  Stop the server: drain queued port messages, TERM, bounded grace, KILL,
  drain again, confirm reaped — then release the data-dir claim so the
  documented restart-on-the-same-dir flow keeps working.

  The claim is released ONLY when cleanup is proven AND this instance
  still owns the claim: the live registration must be this start's
  holder process, so a stale handle stopped after a replacement start
  cannot release the replacement's claim, and an unproven cleanup
  (`{:error, :unreaped}` and friends) leaves the exclusion in place
  while the child may still hold the store — it clears when the owning
  process dies.

  Release itself is a bounded synchronous handshake: normally `stop/1`
  does not return until the registration is removed. If confirmation
  times out (a holder that cannot run), the public result preserves
  BOTH facts — the child cleanup proof and the unresolved release —
  as `{:error, {:claim_release_unconfirmed, holder, cleanup}}`; the
  claim clears when the holder next runs or its owner dies, and an
  immediate same-directory restart may still be refused until then.

  Returns `{:exit_status, n}` only for an observed OS exit code;
  `{:reaped, :status_unobserved}` when the child is confirmed gone but no
  exit_status message was observed; `{:error, ...}` when cleanup cannot be
  proven or the claim release is unconfirmed.
  """
  @spec stop(t()) :: {:exit_status, integer()} | {:reaped, :status_unobserved} | {:error, term()}
  def stop(%__MODULE__{} = server) do
    result = stop_server(server)

    case release_claim_if_earned(server, result) do
      :ok ->
        result

      {:error, {:release_unconfirmed, holder}} ->
        {:error, {:claim_release_unconfirmed, holder, result}}
    end
  end

  defp release_claim_if_earned(server, result) do
    proven? = match?({:exit_status, _}, result) or result == {:reaped, :status_unobserved}

    if proven? and is_pid(server.claim_pid) and
         :global.whereis_name(claim_key(server.dir)) == server.claim_pid do
      release_holder(server.claim_pid)
    else
      :ok
    end
  end

  defp stop_server(server) do
    server = drain(server, 0)

    cond do
      server.exit_status != nil ->
        {:exit_status, server.exit_status}

      not alive?(server) ->
        safe_port_close(server.port)
        {:reaped, :status_unobserved}

      true ->
        _ = term(server.os_pid)

        case await_exit(server, @term_grace_ms) do
          {:timeout, server} ->
            # Honest diagnostics for a real graceful-stop failure: the
            # child's signal mask/state, open-FD count and wait channel
            # from /proc. Evidence only — no control flow depends on it.
            safe_puts("test_server TERM grace exceeded: #{signal_state(server.os_pid)}")

            # If the port's owning process died, exit_status messages were
            # lost — a dead child with a closed port is honestly reported
            # as reaped-without-status, never as a fabricated code.
            #
            # Per SurrealDB v3.0.5 signals.rs, a SECOND TERM forces
            # immediate HTTP/RPC shutdown + task cancellation — it is
            # not a second graceful attempt. The only forced outcome
            # here is KILL, reported with its real exit status.
            if alive?(server),
              do: kill_and_reap(server),
              else:
                (
                  safe_port_close(server.port)
                  {:reaped, :status_unobserved}
                )

          {server, status} ->
            report_exit(server, status)
        end
    end
  end

  defp term(os_pid) do
    System.cmd("kill", ["-TERM", Integer.to_string(os_pid)], stderr_to_stdout: true)
  end

  defp report_exit(server, status) do
    server = drain(server, 100)

    if alive?(server), do: {:error, :still_alive}, else: {:exit_status, status}
  end

  defp kill_and_reap(server) do
    _ =
      System.cmd("kill", ["-KILL", Integer.to_string(server.os_pid)], stderr_to_stdout: true)

    case await_exit(server, @kill_grace_ms) do
      {:timeout, server} ->
        safe_port_close(server.port)

        if alive?(server),
          do: {:error, :unreaped},
          else: {:reaped, :status_unobserved}

      {server, status} ->
        report_exit(server, status)
    end
  end

  # Bounded diagnostics of the un-reaped child. /proc is Linux-only; on
  # other platforms (macOS) fall back to `ps` for the state instead of
  # printing three Linux-shaped "unreadable" placeholders.
  defp signal_state(os_pid) do
    pid = Integer.to_string(os_pid)

    if File.dir?("/proc") do
      base =
        case File.read("/proc/#{os_pid}/status") do
          {:ok, status} ->
            status
            |> String.split("\n")
            |> Enum.filter(&String.starts_with?(&1, ["State:", "SigBlk:", "SigIgn:", "SigCgt:"]))
            |> Enum.join(" ")
            |> String.slice(0, 240)

          {:error, r} ->
            "status unreadable: #{inspect(r)}"
        end

      fds =
        case File.ls("/proc/#{os_pid}/fd") do
          {:ok, l} -> "fds=#{length(l)}"
          _ -> "fds=?"
        end

      wchan =
        case File.read("/proc/#{os_pid}/wchan") do
          {:ok, w} -> "wchan=#{String.trim(w)}"
          _ -> "wchan=?"
        end

      "#{base} #{fds} #{wchan}"
    else
      case System.cmd("ps", ["-o", "stat=,rss=", "-p", pid], stderr_to_stdout: true) do
        {out, 0} -> "ps: #{String.slice(String.trim(out), 0, 240)}"
        _ -> "signal state unavailable on this platform for pid #{pid}"
      end
    end
  end

  defp safe_port_close(port) do
    try do
      Port.close(port)
    rescue
      _ -> :ok
    end
  end

  # Until an exit status is observed, conservatively treat the child as
  # possibly alive. Port.info/2 returns nil for a closed port, not
  # :undefined; neither a closed port nor absent /proc proves child exit.
  # This test helper retains an unknown cleanup outcome on that path.
  defp alive?(%__MODULE__{exit_status: nil}), do: true

  defp alive?(%__MODULE__{}), do: false

  defp await_exit(server, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    receive do
      {port, {:exit_status, status}} when port == server.port ->
        {%{server | exit_status: status}, status}

      {port, {:data, d}} when port == server.port ->
        server = append_log(server, d)
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining <= 0,
          do: {:timeout, server},
          else: await_exit(server, remaining)
    after
      timeout -> {:timeout, server}
    end
  end

  @doc """
  Drain port output into server.log and return the updated server.

  Always performs a nonblocking burst pass first (bounded at
  #{@max_drain_items} queued items) so an already-queued exit_status is
  observed even with `timeout: 0`; then blocks up to `timeout` for trailing
  output. Call inside long wait loops so diagnostic bytes never accumulate
  unboundedly in the port mailbox.
  """
  @spec drain(t(), non_neg_integer()) :: t()
  def drain(server, timeout \\ 100) do
    server = burst_drain(server, @max_drain_items)

    if server.exit_status != nil or timeout <= 0 do
      server
    else
      deadline = System.monotonic_time(:millisecond) + timeout
      block_drain(server, deadline, @max_drain_items)
    end
  end

  # Nonblocking: always attempts receive with `after 0`.
  defp burst_drain(server, 0), do: server

  defp burst_drain(server, budget) do
    receive do
      {port, {:data, d}} when port == server.port ->
        burst_drain(append_log(server, d), budget - 1)

      {port, {:exit_status, status}} when port == server.port ->
        %{server | exit_status: status}
    after
      0 -> server
    end
  end

  defp block_drain(server, _deadline, 0), do: server

  defp block_drain(server, deadline, budget) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      server
    else
      receive do
        {port, {:data, d}} when port == server.port ->
          block_drain(append_log(server, d), deadline, budget - 1)

        {port, {:exit_status, status}} when port == server.port ->
          %{server | exit_status: status}
      after
        remaining -> server
      end
    end
  end

  # Cumulative byte cap: server.log never exceeds @max_log_bytes.
  defp append_log(server, data) do
    if server.log_bytes >= @max_log_bytes do
      %{server | log_bytes: server.log_bytes + byte_size(data)}
    else
      room = @max_log_bytes - server.log_bytes
      chunk = binary_part(data, 0, min(byte_size(data), room))
      File.write(server.log_path, chunk, [:append])
      %{server | log_bytes: server.log_bytes + byte_size(data)}
    end
  end
end
