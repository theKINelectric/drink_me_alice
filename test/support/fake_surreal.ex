defmodule DrinkMeAlice.FakeSurreal do
  @moduledoc """
  Scriptable loopback peer for `DrinkMeAlice.Conn` tests: a real
  WebSocket (via `SynthPeer`) answering JSON-RPC requests with a
  caller-supplied handler. This is a *test fixture* — it validates the
  client's transport/session behavior; it is not SurrealDB evidence and
  never substitutes for the real-server campaign.

  `handler :: fn(method :: binary(), params :: term()) -> reply`
  where reply is `{:reply, term()}` | `{:sleep, ms, term()}` | `:drop`.

  Commands (acknowledged):
    * `{:set_handler, fun}`
    * `{:notify, body_map}` — send one live-notification text frame
      `{"result": body}` (the 3.0.5 live-notification shape).
    * `:close_tcp`
  """

  alias DrinkMeAlice.{Json, SynthPeer}

  defstruct [:peer, :handler]

  @doc "Start a fake; returns {:ok, fake, port}."
  def start(handler \\ &default/2) do
    {:ok, peer, port} = SynthPeer.start()
    parent = self()

    pid =
      spawn_link(fn ->
        # Push mode: the peer forwards every parsed client frame as
        # {:synth_frame, ...}. `expect_frame/2` is unusable as a poll —
        # a timed-out expect is fatal to the peer by design.
        SynthPeer.forward_to(peer, self())
        loop(%__MODULE__{peer: peer, handler: handler})
      end)

    {:ok, %{pid: pid, peer: peer, port: port, parent: parent}}
  end

  @doc "Default canned replies for the standard session sequence."
  def default("signin", _params), do: {:reply, nil}
  def default("use", _params), do: {:reply, nil}
  def default("live", _params), do: {:reply, "fake-live-uuid-1"}

  def default("query", _params) do
    {:reply,
     [
       %{
         "status" => "OK",
         "result" => %{
           "now" => "2026-01-01T00:00:00Z",
           "floor" => "2026-01-01T00:00:00Z",
           "regressed" => false,
           "ok" => true
         }
       }
     ]}
  end

  def default(_method, _params), do: {:reply, nil}

  @doc "Replace the request handler."
  def command(fake, cmd) do
    ref = make_ref()
    send(fake.pid, {:cmd, self(), ref, cmd})

    receive do
      {^ref, :ok} -> :ok
    after
      2_000 -> {:error, :fake_timeout}
    end
  end

  @doc "Send a live-notification frame carrying `body` as `result`."
  def notify(fake, body), do: command(fake, {:notify, body})

  @doc "Close the underlying TCP (peer-side network failure)."
  def close_tcp(fake), do: command(fake, :close_tcp)

  def stop(fake) do
    send(fake.pid, :stop)
    SynthPeer.stop(fake.peer)
  end

  # -- loop ----------------------------------------------------------------

  defp loop(state) do
    receive do
      :stop ->
        :ok

      {:cmd, from, ref, cmd} ->
        state = run_cmd(state, cmd)
        send(from, {ref, :ok})
        loop(state)

      {:synth_frame, {1, _fin, payload}} ->
        handle_request(state, payload)
        loop(state)

      {:synth_frame, _non_text} ->
        loop(state)

      :synth_closed ->
        :ok

      {:synth_error, _reason} ->
        :ok
    end
  end

  defp run_cmd(state, {:set_handler, fun}), do: %{state | handler: fun}

  defp run_cmd(state, {:notify, body}) do
    send_text(state.peer, %{"result" => body})
    state
  end

  defp run_cmd(state, :close_tcp) do
    SynthPeer.close_tcp(state.peer)
    state
  end

  defp handle_request(state, payload) do
    case Json.decode(payload) do
      {:ok, %{"id" => id, "method" => method, "params" => params}} ->
        case state.handler.(method, params) do
          {:reply, result} ->
            send_text(state.peer, %{"id" => id, "result" => result})

          {:sleep, ms, result} ->
            Process.sleep(ms)
            send_text(state.peer, %{"id" => id, "result" => result})

          {:error_reply, code, message} ->
            send_text(state.peer, %{
              "id" => id,
              "error" => %{"code" => code, "message" => message}
            })

          :drop ->
            :ok
        end

      _other ->
        :ok
    end
  end

  defp send_text(peer, map) do
    {:ok, raw} = Json.encode(map)
    SynthPeer.send_frame(peer, 1, true, raw)
  end
end
