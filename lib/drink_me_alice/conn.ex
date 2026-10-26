defmodule DrinkMeAlice.Conn do
  @moduledoc """
  The single supervised connection owner: one Mint/MintWebSocket
  connection, one outstanding RPC at a time, bounded admission via
  `DrinkMeAlice.Credits` *before* any mailbox send.

  ## Outcome vocabulary (transport facts only)

  `call/3` returns transport-level facts; `DrinkMeAlice.Tx.run/4` (or a
  caller's own port) maps them to write outcomes:

  - `{:reply, decoded}` — a reply for this request id was received and
    decoded under the bounded profile.
  - `{:lost, :timeout | :closed | :transport | :send_failed | :down}` —
    the request was sent (or may have been); no classifying reply arrived
    before the deadline. For mutations this is `{:unknown, request}` —
    never a claim of failure.
  - `{:refused, :down | :expired | :exhausted | :too_large | :encode |
    :clock_uncertain}` — certified non-send: the request provably never
    entered a write path (connection down before send, caller deadline
    already passed while queued, credit bound reached, payload or
    encoding bound, or — for `kind: :mutation` requests — clock
    uncertainty latched at dispatch time). For mutations this is
    `{:unavailable, error}`.

  ## Serialization

  Requests are handled one at a time — the single socket is FIFO and a
  serial owner cannot interleave writes. A caller's deadline is carried
  in the request; a request whose deadline expired *before* its send is
  refused (`:expired`) rather than submitted late, so `unavailable`
  always certifies non-submission. A caller that dies while queued is
  swept by `Credits.sweep/1`; a caller that dies after send gets its
  outcome discarded — the durable side is unaffected.

  ## Notifications as hints only

  An optional `live` subscription on one configured table delivers
  notifications that set a bounded `hint_pending` flag and fan out to at
  most 16 subscriber pids as `{hint_tag, table, generation}`.
  Notifications never mutate durable reads — callers always read afresh. A notification whose live-id is not the current
  subscription's is stale (3.0.5's LIVE defect can deliver post-invalidation
  on a socket we did not replace); it is counted and discarded. On
  session expiry or auth failure the whole socket is closed and a new
  connection authenticates/selects/resubscribes — the adapter does not
  switch identities on a live-subscription socket.

  ## Clock authority (optional)

  When `clock_query` is configured, establishment reads it — a query
  whose single result is `%{"now" => string, "floor" => _, "regressed" =>
  boolean}` — and every reply is scanned for a `regressed: true` result
  or a `clock_regressed` decision. Either latches `clock_uncertain` for
  this connection generation; a mutation (`kind: :mutation`) is refused
  at dispatch — serially in this owner, immediately before its frame is
  written — while the latch holds. Reads are never gated.
  `clock_authority/1` reads the latch for introspection only; it is not
  part of any write path. Without `clock_query` no floor is read and
  nothing latches.

  ## Configuration

  - `host`, `port`, `ns`, `db`, `user`, `pass` — required.
  - `signin` — `:database` (default: a database-scoped user, the
    least-privilege runtime identity), `:namespace` or `:root`.
  - `live` — `nil` (default) or a table name to subscribe for hints.
  - `hint_tag` — the atom tagging hint messages (default
    `:drink_me_alice_hint`).
  - `clock_query` — `nil` (default) or `{sql, vars}` as above.
  - `poll_ms` (default 50), `connect_timeout` (default 2_000).
  - `name` — optional registered name.
  """

  use GenServer

  alias DrinkMeAlice.{Credits, Json, Outcome, Ws}

  @rpc_deadline_ms 2_000
  @call_slack_ms 500
  @max_request_external 1_048_576
  @poll_ms 50
  @backoff_min 250
  @backoff_max 5_000
  @max_abandoned 32
  @max_hint_subs 16
  @max_consecutive_timeouts 3

  # Crash-log redaction (review 27): an unexpected callback failure makes
  # OTP print this struct in the exception arguments AND the report's
  # State line. The derived Inspect whitelists only safe metadata — the
  # config (runtime password), live ws/parser buffers, and raw request
  # data can never reach a log through either path.
  @derive {Inspect,
           only: [
             :status,
             :generation,
             :reconnects,
             :next_id,
             :backoff,
             :consecutive_timeouts,
             :hint_pending,
             :hint_seen,
             :notifications_seen,
             :notifications_stale,
             :clock_floor,
             :clock_uncertain,
             :last_error,
             :live_uuid
           ]}
  defstruct ws: nil,
            config: nil,
            credit_tab: nil,
            status: :disconnected,
            backoff: @backoff_min,
            next_id: 1,
            live_uuid: nil,
            abandoned: MapSet.new(),
            consecutive_timeouts: 0,
            hint_subs: MapSet.new(),
            hint_pending: false,
            hint_seen: 0,
            notifications_seen: 0,
            notifications_stale: 0,
            clock_floor: nil,
            clock_uncertain: false,
            generation: 0,
            reconnects: 0,
            last_error: nil,
            fault: %{}

  @type status :: :disconnected | :reconciling | :healthy

  @type request :: %{
          required(:method) => String.t(),
          required(:params) => list() | map(),
          optional(:kind) => :mutation | :read
        }

  # -- public API -------------------------------------------------------------

  @doc """
  Start the owner. See the module doc for `config` keys. An invalid
  configuration is refused before any process starts:
  `{:error, {:invalid_config, key}}`.
  """
  @spec start_link(keyword() | map()) :: {:ok, pid()} | {:error, term()}
  def start_link(config) do
    {name, config} = split_name(config)

    with :ok <- check_config(config) do
      GenServer.start_link(__MODULE__, config, name: name)
    end
  end

  @spec start(keyword() | map()) :: {:ok, pid()} | {:error, term()}
  def start(config) do
    {name, config} = split_name(config)

    with :ok <- check_config(config) do
      GenServer.start(__MODULE__, config, name: name)
    end
  end

  @doc false
  def child_spec(config) do
    id = config |> Map.new() |> Map.get(:name, __MODULE__)
    %{id: id, start: {__MODULE__, :start_link, [config]}}
  end

  defp check_config(c) do
    poll_ms = Map.get(c, :poll_ms, @poll_ms)

    cond do
      not (is_binary(c[:host]) and byte_size(c[:host]) in 1..255) ->
        {:error, {:invalid_config, :host}}

      not (is_integer(c[:port]) and c[:port] in 1..65_535) ->
        {:error, {:invalid_config, :port}}

      not DrinkMeAlice.identifier?(c[:ns]) ->
        {:error, {:invalid_config, :ns}}

      not DrinkMeAlice.identifier?(c[:db]) ->
        {:error, {:invalid_config, :db}}

      not (is_binary(c[:user]) and byte_size(c[:user]) in 1..256) ->
        {:error, {:invalid_config, :user}}

      not (is_binary(c[:pass]) and byte_size(c[:pass]) in 1..1_024) ->
        {:error, {:invalid_config, :pass}}

      Map.get(c, :signin, :database) not in [:database, :namespace, :root] ->
        {:error, {:invalid_config, :signin}}

      not (Map.get(c, :live) == nil or DrinkMeAlice.identifier?(c[:live])) ->
        {:error, {:invalid_config, :live}}

      not is_atom(Map.get(c, :hint_tag, :drink_me_alice_hint)) ->
        {:error, {:invalid_config, :hint_tag}}

      not clock_query?(Map.get(c, :clock_query)) ->
        {:error, {:invalid_config, :clock_query}}

      not (is_integer(poll_ms) and poll_ms in 1..1_000) ->
        {:error, {:invalid_config, :poll_ms}}

      true ->
        :ok
    end
  end

  defp clock_query?(nil), do: true
  defp clock_query?({sql, vars}) when is_binary(sql) and is_map(vars), do: true
  defp clock_query?(_), do: false

  # `config` is advertised as keyword OR map — normalize both once here.
  defp split_name(config) when is_map(config), do: {Map.get(config, :name), config}
  defp split_name(config) when is_list(config), do: {Keyword.get(config, :name), Map.new(config)}

  @doc """
  Send one RPC under the caller's deadline (default 2 s — the ordinary
  RPC bound). Reserves a credit slot first; see the module doc for the
  returned transport facts.

  The request payload is installed into the reserved slot atomically
  (`Credits.enqueue/3` — one CAS that publishes payload+state together).
  The owner drains `:enqueued` rows on a fixed `poll_ms` cadence — no
  caller→owner message exists, so the owner mailbox stays bounded under
  arbitrarily delayed callers. The owner frees the slot when it consumes
  the row (replied or deadline-refused): a caller that times out or dies
  never releases, and a dead holder's `:reserved`-only slot is reclaimed
  by `Credits.sweep/1`. No timing window can orphan a queued payload.
  """
  @spec call(GenServer.server(), request(), keyword()) ::
          {:reply, map()} | {:lost, atom() | tuple()} | {:refused, atom()}
  def call(conn, request, opts \\ []) do
    deadline_ms = min(Keyword.get(opts, :deadline_ms, @rpc_deadline_ms), @rpc_deadline_ms)
    class = Keyword.get(opts, :class, :general)

    with {:ok, pid, tab} <- resolve(conn),
         :ok <- check_size(request),
         {:ok, token} <- Credits.reserve(tab, class) do
      dispatch(pid, tab, request, deadline_ms, token)
    else
      {:error, :no_conn} -> {:refused, :down}
      {:error, :no_table} -> {:refused, :down}
      {:error, :exhausted} -> {:refused, :exhausted}
      {:error, :too_large} -> {:refused, :too_large}
    end
  end

  # The payload bound is enforced *before* the reservation so the slot
  # table never holds an oversized request: `external_size` counts the
  # full term including binary payloads (unlike `flat_size`).
  defp check_size(request) do
    if :erlang.external_size(request) <= @max_request_external,
      do: :ok,
      else: {:error, :too_large}
  end

  # Install the payload into the slot row — the owner's fixed poll
  # drains `:enqueued` rows; there is deliberately no caller→owner
  # message (a caller paused between any counter bump and its send can
  # inject a stale doorbell at an arbitrary later time — only polling
  # bounds the mailbox). The reply destination is a `Process.alias` —
  # the owner sends `{alias, reply}`; if the call times out first,
  # `unalias` destroys the destination so a *future* late reply is
  # dropped silently. A reply that already landed around the timeout
  # boundary is flushed explicitly after deactivation — unalias does not
  # erase queued messages. The owner monitor and the alias are cleaned
  # on every exit path — repeated timeouts in a long-lived caller leak
  # neither.
  defp dispatch(pid, tab, request, deadline_ms, token) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    alias = Process.alias()
    mref = Process.monitor(pid)

    case Credits.enqueue(tab, token, {alias, request, deadline}) do
      :ok ->
        try do
          receive do
            {^alias, reply} ->
              reply

            {:DOWN, ^mref, :process, ^pid, _reason} ->
              {:lost, :down}
          after
            deadline_ms + @call_slack_ms ->
              {:lost, :timeout}
          end
        after
          Process.demonitor(mref, [:flush])
          Process.unalias(alias)
          # A reply that landed around the timeout boundary is still in
          # the mailbox — unalias stops only future delivery.
          receive do
            {^alias, _reply} -> :ok
          after
            0 -> :ok
          end
        end

      {:error, _reason} ->
        Process.demonitor(mref, [:flush])
        Process.unalias(alias)
        _ = Credits.release(tab, token)
        {:refused, :down}
    end
  end

  @doc "Connection status: :disconnected | :reconciling | :healthy."
  @spec status(GenServer.server()) :: status()
  def status(conn), do: GenServer.call(conn, :status, 5_000)

  @doc "Cached clock authority for introspection: :trusted | :uncertain. Not a write-path gate — mutations are gated at dispatch in this owner."
  @spec clock_authority(GenServer.server()) :: :trusted | :uncertain
  def clock_authority(conn), do: GenServer.call(conn, :clock_authority, 5_000)

  @doc "Subscribe `pid` (default caller) to work-item hints. Bounded at 16."
  @spec hint_subscribe(GenServer.server(), pid()) :: :ok | {:error, :full}
  def hint_subscribe(conn, pid \\ self()), do: GenServer.call(conn, {:hint_subscribe, pid}, 5_000)

  @doc "Take the coalesced dirty hint: clears the flag, returns times seen."
  @spec hint_take(GenServer.server()) :: {:hint, non_neg_integer()} | :clean
  def hint_take(conn), do: GenServer.call(conn, :hint_take, 5_000)

  @doc "Notification/health counters for evidence — no durable claims."
  @spec stats(GenServer.server()) :: map()
  def stats(conn), do: GenServer.call(conn, :stats, 5_000)

  @doc """
  Test-only transport fault seam — honest socket-level events, never a
  bypass of the transaction path:

  - `:kill_socket` — close the TCP transport now (network-failure shape).
  - `:drop_after_send` — after the next request frame is written, close
    the transport before the reply can arrive (lost-reply shape).
  - `:drop_reply` — receive the next request's reply, then close the
    transport and report `{:lost, :reply_dropped}` WITHOUT delivering
    it: a reply arrived but is discarded before classification — it
    does not prove the write committed — so the caller observes a real
    lost-reply outcome.

  Session-expiry evidence uses REAL server expiry — `FOR SESSION`
  credentials issued through `Admin` helpers — not a transport seam.
  """
  @spec inject(GenServer.server(), :kill_socket | :drop_after_send | :drop_reply) :: :ok
  def inject(conn, fault), do: GenServer.call(conn, {:inject, fault}, 5_000)

  @doc "The credits table id for `Credits.snapshot/1` evidence."
  @spec credits(GenServer.server()) :: {:ok, :ets.tid()} | {:error, :no_conn}
  def credits(conn) do
    case resolve(conn) do
      {:ok, _pid, tab} -> {:ok, tab}
      {:error, :no_conn} -> {:error, :no_conn}
    end
  end

  # Owner pid + its live credit table. Both are re-read per call — a
  # restarted owner has a fresh table; stale table handles fail closed.
  defp resolve(conn) do
    pid = GenServer.whereis(conn)

    if pid == nil do
      {:error, :no_conn}
    else
      case :persistent_term.get({__MODULE__, pid}, nil) do
        nil -> {:error, :no_conn}
        tab -> {:ok, pid, tab}
      end
    end
  end

  # -- GenServer ---------------------------------------------------------------

  @impl true
  def init(config) do
    config = Map.new(config)
    tab = Credits.new()
    :persistent_term.put({__MODULE__, self()}, tab)
    Process.flag(:trap_exit, true)
    send(self(), :reconnect)
    schedule_poll(config)

    {:ok,
     %__MODULE__{
       config: config,
       credit_tab: tab,
       status: :disconnected
     }}
  end

  @impl true
  def terminate(_reason, state) do
    :persistent_term.erase({__MODULE__, self()})
    _ = state.ws && Ws.close(state.ws)
    _ = state.credit_tab && sweep_all(state.credit_tab)
    :ok
  end

  defp sweep_all(tab) do
    :ets.delete_all_objects(tab)
    :ok
  end

  @impl true
  def handle_call(:status, _from, state), do: {:reply, state.status, state}

  def handle_call(:config, _from, state), do: {:reply, state.config, state}

  def handle_call(:clock_authority, _from, state) do
    {:reply, if(state.clock_uncertain, do: :uncertain, else: :trusted), state}
  end

  def handle_call({:hint_subscribe, pid}, _from, state) do
    cond do
      # Idempotent for a subscriber already registered: exactly one
      # monitor is retained per PID, no matter how often it re-joins.
      # (A repeat call used to stack a fresh monitor behind the set-size
      # bound — unbounded monitors for a long-lived caller. A-04.)
      MapSet.member?(state.hint_subs, pid) ->
        {:reply, :ok, state}

      MapSet.size(state.hint_subs) >= @max_hint_subs ->
        {:reply, {:error, :full}, state}

      true ->
        Process.monitor(pid)
        {:reply, :ok, %{state | hint_subs: MapSet.put(state.hint_subs, pid)}}
    end
  end

  def handle_call(:hint_take, _from, state) do
    if state.hint_pending do
      {:reply, {:hint, state.hint_seen}, %{state | hint_pending: false}}
    else
      {:reply, :clean, state}
    end
  end

  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       status: state.status,
       generation: state.generation,
       reconnects: state.reconnects,
       live_uuid_present: state.live_uuid != nil,
       hint_pending: state.hint_pending,
       hint_seen: state.hint_seen,
       notifications_seen: state.notifications_seen,
       notifications_stale: state.notifications_stale,
       abandoned: MapSet.size(state.abandoned),
       consecutive_timeouts: state.consecutive_timeouts,
       credits: state.credit_tab && Credits.snapshot(state.credit_tab),
       clock_uncertain: state.clock_uncertain,
       clock_floor: state.clock_floor,
       last_error: state.last_error
     }, state}
  end

  def handle_call({:inject, :kill_socket}, _from, state) do
    {:reply, :ok, transport_lost(state, :injected_kill)}
  end

  def handle_call({:inject, :drop_after_send}, _from, state) do
    {:reply, :ok, %{state | fault: Map.put(state.fault, :drop_after_send, true)}}
  end

  def handle_call({:inject, :drop_reply}, _from, state) do
    {:reply, :ok, %{state | fault: Map.put(state.fault, :drop_reply, true)}}
  end

  @impl GenServer
  def format_status(%{state: %__MODULE__{} = s} = status) do
    # Status readers and crash reports get the redacted shape too —
    # the derived Inspect is the belt, this is the suspenders: the
    # credential never reaches a status consumer even in raw form.
    %{status | state: %{s | config: redact_config(s.config), ws: :redacted}}
  end

  def format_status(status), do: status

  defp redact_config(%{} = c) when is_map(c), do: Map.put(c, :pass, "[redacted]")

  defp redact_config(c), do: c

  @impl true
  def handle_info(:poll, state) do
    schedule_poll(state.config)

    _ = state.credit_tab && Credits.sweep(state.credit_tab)
    state = drain(state)

    if state.status == :healthy and state.ws != nil and state.ws.conn != nil do
      case Ws.recv_frames(state.ws, 0) do
        {:ok, ws, frames} ->
          {:noreply, absorb_frames(%{state | ws: ws}, frames, nil)}

        {:timeout, ws} ->
          {:noreply, %{state | ws: ws}}

        {:closed, ws, frames} ->
          state = absorb_frames(%{state | ws: ws}, frames, nil)
          {:noreply, transport_lost(state, :closed)}

        {:error, ws, reason} ->
          {:noreply, transport_lost(%{state | ws: ws}, reason)}
      end
    else
      {:noreply, state}
    end
  end

  def handle_info(:reconnect, state) do
    case connect_and_establish(state) do
      {:ok, state} ->
        # New generation is healthy — consume any requests queued through
        # the outage (their deadlines are re-checked per slot).
        {:noreply, drain(%{state | status: :healthy, backoff: @backoff_min, last_error: nil})}

      {:error, reason, state} ->
        backoff = state.backoff
        Process.send_after(self(), :reconnect, backoff)

        {:noreply,
         %{
           state
           | status: :disconnected,
             backoff: min(backoff * 2, @backoff_max),
             last_error: reason
         }}
    end
  end

  def handle_info({:DOWN, _mref, :process, pid, _reason}, state) do
    state = %{state | hint_subs: MapSet.delete(state.hint_subs, pid)}

    # A dead credit holder is reclaimed eagerly here as well as by the
    # poll sweep; a dead caller's enqueued payload still lives in its
    # slot row and is consumed/freed by `drain/1` — never silently
    # dropped, never double-freed.
    if state.credit_tab, do: Credits.sweep(state.credit_tab)
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # -- connection lifecycle ---------------------------------------------------

  defp connect_and_establish(state) do
    c = state.config

    case Ws.connect(c.host, c.port, "/rpc",
           connect_timeout: Map.get(c, :connect_timeout, 2_000),
           upgrade_timeout: 2_000
         ) do
      {:ok, ws0, early} ->
        # Every establishment step closes the ws it was handed before
        # reporting failure — the `with` `else` cannot see the inner
        # `ws`, so without per-step closing a failed signin/use/live
        # leaks the acquired socket (review-05 FD leak).
        with {:ok, ws, _extra} <- establish_step(ws0, &absorb_early(&1, early)),
             {:ok, ws, _extra} <-
               establish_step(ws, &rpc_now(&1, "signin", [signin_params(c)])),
             {:ok, ws, _extra} <- establish_step(ws, &rpc_now(&1, "use", [c.ns, c.db])),
             {:ok, ws, live_uuid} <- establish_step(ws, &maybe_live(&1, state, c)),
             {:ok, ws, clock} <- establish_step(ws, &read_clock_floor(&1, c)) do
          {:ok,
           %{
             state
             | ws: ws,
               live_uuid: live_uuid,
               generation: state.generation + 1,
               reconnects: state.reconnects + 1,
               abandoned: MapSet.new(),
               consecutive_timeouts: 0,
               clock_floor: clock.floor,
               clock_uncertain: state.clock_uncertain or clock.regressed,
               status: :reconciling
           }}
        else
          {:error, reason} -> {:error, reason, state}
        end

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  # Run an establishment step over the latest `ws`; on failure close THAT
  # ws (the freshest parser/socket state) and report the error. Steps may
  # return `{:ok, ws}` or `{:ok, ws, extra}` — normalize to the 3-tuple.
  defp establish_step(ws, fun) do
    case fun.(ws) do
      {:ok, ws} ->
        {:ok, ws, nil}

      {:ok, ws, extra} ->
        {:ok, ws, extra}

      {:error, r} ->
        _ = Ws.close(ws)
        {:error, r}

      {:lost, r} ->
        _ = Ws.close(ws)
        {:error, {:lost, r}}
    end
  end

  @doc false
  # Signin parameters per scope: database-scoped (the runtime default),
  # namespace-scoped, or root (no NS/DB keys).
  def signin_params(c) do
    case Map.get(c, :signin, :database) do
      :database -> %{"NS" => c.ns, "DB" => c.db, "user" => c.user, "pass" => c.pass}
      :namespace -> %{"NS" => c.ns, "user" => c.user, "pass" => c.pass}
      :root -> %{"user" => c.user, "pass" => c.pass}
    end
  end

  defp absorb_early(ws, []), do: {:ok, ws}

  defp absorb_early(ws, frames) do
    # Early frames during the handshake are drained and counted; a reply
    # can never arrive before our first request, so anything here is a
    # notification or control traffic.
    Enum.reduce_while(frames, {:ok, ws}, fn
      {:notification, _body, _size}, acc -> {:cont, acc}
      {:ping, _}, acc -> {:cont, acc}
      {:pong, _}, acc -> {:cont, acc}
      {:malformed, r}, _acc -> {:halt, {:error, {:early_malformed, r}}}
      {:close, _, _}, _acc -> {:halt, {:error, :early_close}}
      {:text, _}, acc -> {:cont, acc}
      {:binary, _}, acc -> {:cont, acc}
    end)
  end

  # Blocking one-shot RPC used only during connection establishment —
  # runs inside :reconnect, no credits (owner-internal, not a caller send).
  defp rpc_now(ws, method, params) do
    id = :erlang.unique_integer([:positive, :monotonic])
    msg = %{"id" => id, "method" => method, "params" => params}

    with {:ok, raw} <- encode_message(msg),
         {:ok, ws} <- Ws.send_frame(ws, {:text, raw}),
         {:ok, ws, reply} <- await_reply(ws, id, 2_000) do
      case reply do
        %{"error" => %{"code" => code}} -> {:error, {:rpc_error, code}}
        %{"result" => _} -> {:ok, ws}
        _ -> {:error, :bad_reply}
      end
    else
      {:lost, r} -> {:error, {:lost, r}}
      {:error, r} -> {:error, r}
    end
  end

  defp maybe_live(ws, _state, %{live: table}) when is_binary(table) do
    id = :erlang.unique_integer([:positive, :monotonic])
    msg = %{"id" => id, "method" => "live", "params" => [table]}

    with {:ok, raw} <- encode_message(msg),
         {:ok, ws} <- Ws.send_frame(ws, {:text, raw}),
         {:ok, ws, reply} <- await_reply(ws, id, 2_000) do
      case reply do
        %{"result" => uuid} when is_binary(uuid) -> {:ok, ws, uuid}
        %{"error" => %{"code" => code}} -> {:error, {:live_failed, code}}
        _ -> {:error, :live_bad_reply}
      end
    else
      {:lost, r} -> {:error, {:live_lost, r}}
      {:error, r} -> {:error, r}
    end
  end

  defp maybe_live(ws, _state, _config), do: {:ok, ws, nil}

  # Establishment-time floor read: one bounded query returning
  # `%{now, floor, regressed}`; `regressed` is computed in-database.
  # Without a configured `clock_query` there is no floor to read.
  defp read_clock_floor(ws, %{clock_query: {sql, vars}}) do
    id = :erlang.unique_integer([:positive, :monotonic])
    msg = %{"id" => id, "method" => "query", "params" => [sql, vars]}

    with {:ok, raw} <- encode_message(msg),
         {:ok, ws} <- Ws.send_frame(ws, {:text, raw}),
         {:ok, ws, reply} <- await_reply(ws, id, 2_000) do
      case extract_scalar_result(reply) do
        {:ok, %{"now" => now, "floor" => floor, "regressed" => regressed}}
        when is_binary(now) and is_boolean(regressed) ->
          {:ok, ws, %{now: now, floor: floor, regressed: regressed}}

        {:ok, other} ->
          {:error, {:floor_bad_reply, inspect(other) |> String.slice(0, 60)}}

        {:error, r} ->
          {:error, r}
      end
    else
      {:lost, r} -> {:error, {:floor_lost, r}}
      {:error, r} -> {:error, r}
    end
  end

  defp read_clock_floor(ws, _config), do: no_clock_floor(ws)

  defp extract_scalar_result(%{"result" => results}) when is_list(results) do
    Enum.find_value(results, {:error, :no_result}, fn
      %{"status" => "OK", "result" => r} ->
        {:ok, r}

      %{"status" => "ERR", "result" => r} ->
        {:error, {:statement_err, String.slice(to_string(r), 0, 80)}}

      _ ->
        nil
    end)
  end

  defp extract_scalar_result(_), do: {:error, :bad_reply}

  defp no_clock_floor(ws), do: {:ok, ws, %{now: nil, floor: nil, regressed: false}}

  # -- request path -------------------------------------------------------------

  # Consume every *eligible* enqueued slot, oldest enqueue first
  # (`Credits.enqueued/1` order keeps recovery polling fair). A slot is
  # eligible when the owner is healthy — or always once its deadline has
  # passed, so a queued request from a timed-out or dead caller still
  # gets consumed, refused-typed and freed rather than lingering.
  # Unhealthy-but-unexpired slots wait for the next poll/reconnect.
  defp drain(state) do
    case state.credit_tab do
      nil ->
        state

      tab ->
        Enum.reduce(Credits.enqueued(tab), state, fn {n, epoch, _ms, payload}, st ->
          {_alias, _req, deadline} = payload
          now = System.monotonic_time(:millisecond)

          if st.status == :healthy or now >= deadline do
            case Credits.take(tab, n, epoch) do
              :none ->
                st

              {:ok, {alias, req, deadline}} ->
                {reply, st2} =
                  if System.monotonic_time(:millisecond) >= deadline do
                    {{:refused, :expired}, st}
                  else
                    case do_rpc(st, req, deadline) do
                      {:reply, r, s} -> {r, s}
                    end
                  end

                Credits.finish(tab, {n, epoch})
                # Reply to the alias; if the caller already timed out the
                # alias is destroyed and the send drops silently — no
                # stale `{:"$bf_reply"}` accumulates in its mailbox.
                send(alias, {alias, reply})

                # A session-level refusal means the auth on this socket
                # is dead — deliver the reply, then drop the transport so
                # the reconnect path signs in fresh. Generic errors keep
                # the socket.
                case reply do
                  {:reply, %{"error" => %{} = err}} ->
                    if Outcome.session_refusal?(err),
                      do: transport_lost(st2, :session_refused),
                      else: st2

                  _other ->
                    st2
                end
            end
          else
            st
          end
        end)
    end
  end

  # One request in flight; the owner is serial. `req.method`/`req.params`
  # are already validated by the caller (`DrinkMeAlice.Tx` builders or
  # the caller's own bounded SQL).
  #
  # The mutation clock gate lives HERE — under the credit/deadline
  # protocol, serial with the owner's other work, checked immediately
  # before the frame is encoded and written. A preflight control call
  # from the caller could only read a cached flag and would race this
  # latch (uncertainty can arrive after the check but before the send);
  # refusing at dispatch is the certified non-send fact (A-02).
  defp do_rpc(%{clock_uncertain: true} = state, %{kind: :mutation}, _deadline) do
    {:reply, {:refused, :clock_uncertain}, %{state | last_error: :clock_uncertain}}
  end

  defp do_rpc(state, req, deadline) do
    id = state.next_id
    msg = %{"id" => id, "method" => req.method, "params" => req.params}

    case encode_message(msg) do
      {:error, reason} ->
        {:reply, {:refused, :encode}, %{state | last_error: reason}}

      {:ok, raw} ->
        case Ws.send_frame(state.ws, {:text, raw}) do
          {:error, ws, reason} ->
            # A failed send may have written a partial frame — the request
            # may have entered the write path, so this is lost, not refused.
            state = transport_lost(%{state | ws: ws}, reason)
            {:reply, {:lost, :send_failed}, state}

          {:ok, ws} ->
            state = %{state | ws: ws, next_id: id + 1}

            if Map.get(state.fault, :drop_after_send) do
              # Test seam: honest socket death after the write left us.
              state =
                transport_lost(
                  %{state | fault: Map.delete(state.fault, :drop_after_send)},
                  :injected_drop
                )

              {:reply, {:lost, :closed}, state}
            else
              await_rpc(state, id, deadline)
            end
        end
    end
  end

  defp await_rpc(state, id, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      rpc_timed_out(state, id)
    else
      case Ws.recv_frames(state.ws, min(remaining, 250)) do
        {:ok, ws, frames} ->
          state = %{state | ws: ws}

          case scan_frames(state, frames, id) do
            {:reply, reply, state} ->
              {:reply, {:reply, reply}, %{state | consecutive_timeouts: 0}}

            {:continue, state} ->
              await_rpc(state, id, deadline)

            {:lost, reason, state} ->
              {:reply, {:lost, reason}, state}
          end

        {:timeout, ws} ->
          await_rpc(%{state | ws: ws}, id, deadline)

        {:closed, ws, frames} ->
          state = absorb_frames(%{state | ws: ws}, frames, id)
          {:reply, {:lost, :closed}, transport_lost(state, :closed)}

        {:error, ws, reason} ->
          {:reply, {:lost, :transport}, transport_lost(%{state | ws: ws}, reason)}
      end
    end
  end

  # Fold one batch of frames looking for our reply id.
  defp scan_frames(state, frames, id) do
    Enum.reduce_while(frames, {:continue, state}, fn frame, {:continue, st} ->
      case frame do
        {:text, raw} ->
          case Json.decode(raw) do
            {:ok, %{"id" => ^id} = reply} ->
              if Map.get(st.fault, :drop_reply) do
                # Test seam: a reply DID arrive, but it is discarded
                # before classification — arrival alone does not prove
                # the write committed. The caller sees a real
                # lost-reply outcome rather than a fabricated commit.
                st =
                  transport_lost(
                    %{st | fault: Map.delete(st.fault, :drop_reply)},
                    :injected_drop_reply
                  )

                {:halt, {:lost, :reply_dropped, st}}
              else
                {:halt, {:reply, reply, latch_clock(st, reply)}}
              end

            {:ok, %{"id" => other}} ->
              if MapSet.member?(st.abandoned, other) do
                {:cont, {:continue, %{st | abandoned: MapSet.delete(st.abandoned, other)}}}
              else
                {:cont, {:continue, st}}
              end

            {:ok, _non_reply} ->
              {:cont, {:continue, st}}

            {:error, _decode_error} ->
              {:halt, {:lost, :malformed, st}}
          end

        {:notification, body, _size} ->
          {:cont, {:continue, absorb_notification(st, body)}}

        {:malformed, _reason} ->
          {:halt, {:lost, :malformed, st}}

        {:close, _c, _r} ->
          {:halt, {:lost, :closed, st}}

        _other ->
          {:cont, {:continue, st}}
      end
    end)
  end

  # Frames drained outside an awaited reply (idle poll / closed batch).
  defp absorb_frames(state, frames, awaited_id) do
    Enum.reduce(frames, state, fn
      {:notification, body, _size}, st ->
        absorb_notification(st, body)

      {:text, raw}, st ->
        case Json.decode(raw) do
          {:ok, %{"id" => rid}} when rid == awaited_id -> st
          {:ok, %{"id" => rid}} -> %{st | abandoned: MapSet.delete(st.abandoned, rid)}
          _ -> st
        end

      _frame, st ->
        st
    end)
  end

  defp rpc_timed_out(state, id) do
    state = %{state | consecutive_timeouts: state.consecutive_timeouts + 1}
    abandoned = MapSet.put(state.abandoned, id)

    state =
      if MapSet.size(abandoned) > @max_abandoned or
           state.consecutive_timeouts >= @max_consecutive_timeouts do
        # A socket that repeatedly cannot produce replies is not trusted
        # for further correlation — replace it.
        transport_lost(state, :timeout_streak)
      else
        %{state | abandoned: abandoned}
      end

    {:reply, {:lost, :timeout}, state}
  end

  defp transport_lost(state, reason) do
    _ = state.ws && Ws.close(state.ws)
    Process.send_after(self(), :reconnect, state.backoff)

    %{
      state
      | ws: nil,
        status: :disconnected,
        live_uuid: nil,
        abandoned: MapSet.new(),
        consecutive_timeouts: 0,
        hint_pending: false,
        last_error: reason,
        backoff: state.backoff
    }
  end

  # -- notifications -------------------------------------------------------------

  # A matching-generation notification flips clean → dirty and fans out
  # exactly once per edge. While dirty, further notifications only bump
  # the seen counter — subscribers hold at most one pending hint each.
  # `hint_take/1` is the acknowledgement: it clears the flag, and the
  # next matching notification re-arms the edge.
  defp absorb_notification(state, body) do
    state = %{state | notifications_seen: state.notifications_seen + 1}

    if state.live_uuid != nil and Map.get(body, "id") == state.live_uuid do
      if state.hint_pending do
        state
      else
        fanout_hint(state)
        %{state | hint_seen: state.hint_seen + 1, hint_pending: true}
      end
    else
      # Stale-generation notification (e.g. a subscription id this socket
      # no longer owns — the 3.0.5 LIVE defect can deliver after
      # invalidation): counted, never applied.
      %{state | notifications_stale: state.notifications_stale + 1}
    end
  end

  defp fanout_hint(state) do
    tag = Map.get(state.config, :hint_tag, :drink_me_alice_hint)
    table = Map.get(state.config, :live)

    Enum.each(state.hint_subs, fn pid ->
      send(pid, {tag, table, state.generation})
    end)
  end

  # -- clock authority -------------------------------------------------------------

  # With a configured `clock_query`, every statement-result map in every
  # reply is checked for the in-database `regressed` flag or a
  # `clock_regressed` verdict; either latches uncertainty for this
  # connection generation. Without one, application rows that happen to
  # carry such keys never latch anything.
  defp latch_clock(%{config: %{clock_query: {_, _}}} = state, %{"result" => results})
       when is_list(results) do
    flagged =
      Enum.any?(results, fn
        %{"result" => %{"regressed" => true}} -> true
        %{"result" => %{"decision" => "clock_regressed"}} -> true
        _ -> false
      end)

    if flagged, do: %{state | clock_uncertain: true}, else: state
  end

  defp latch_clock(state, _reply), do: state

  defp schedule_poll(config) do
    Process.send_after(self(), :poll, Map.get(config || %{}, :poll_ms, @poll_ms))
  end

  defp encode_message(msg) do
    case Json.encode(msg) do
      {:ok, raw} when byte_size(raw) <= 1_048_576 -> {:ok, raw}
      {:ok, _raw} -> {:error, :request_too_large}
      {:error, err} -> {:error, {:encode_failed, err.code}}
    end
  end

  # Await helper for establishment-time one-shot RPCs.
  defp await_reply(ws, id, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_reply(ws, id, deadline)
  end

  defp do_await_reply(ws, id, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:lost, :timeout}
    else
      case Ws.recv_frames(ws, min(remaining, 250)) do
        {:ok, ws, frames} ->
          case find_reply(frames, id) do
            {:found, reply} -> {:ok, ws, reply}
            :not_found -> do_await_reply(ws, id, deadline)
            {:fatal, r} -> {:lost, r}
          end

        {:timeout, ws} ->
          do_await_reply(ws, id, deadline)

        {:closed, _ws, _frames} ->
          {:lost, :closed}

        {:error, _ws, r} ->
          {:lost, r}
      end
    end
  end

  defp find_reply(frames, id) do
    Enum.reduce_while(frames, :not_found, fn
      {:text, raw}, _acc ->
        case Json.decode(raw) do
          {:ok, %{"id" => ^id} = reply} -> {:halt, {:found, reply}}
          {:ok, _other} -> {:cont, :not_found}
          {:error, _} -> {:halt, {:fatal, :malformed}}
        end

      {:malformed, _}, _acc ->
        {:halt, {:fatal, :malformed}}

      {:close, _, _}, _acc ->
        {:halt, {:fatal, :closed}}

      _frame, _acc ->
        {:cont, :not_found}
    end)
  end
end
