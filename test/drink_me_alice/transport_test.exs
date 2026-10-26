defmodule DrinkMeAlice.TransportTest do
  @moduledoc """
  Local `FrameGuard`/`Ws` boundary regressions (A-08): the pure framing
  guard at its actual limits, and the transport over a REAL socket
  (SynthPeer loopback) — fragmented oversized messages, malformed
  control frames, declared-length attacks, ping floods, close echo.
  Every rejection case has a nearby valid control, and every rejection
  must also release the owned transport (A-05's honesty requirement at
  the layer this library owns).
  """

  use ExUnit.Case, async: false

  alias DrinkMeAlice.{FrameGuard, SynthPeer, Ws}

  @max 1_000

  # -- FrameGuard (pure) -------------------------------------------------------

  describe "FrameGuard" do
    test "a declared payload over max is refused before any body bytes" do
      g = FrameGuard.new(max_message: @max)
      # Header alone declares 1_001 — no body has arrived yet.
      header = <<1::1, 0::3, 1::4, 0::1, 126::7, 1001::16-unsigned>>
      assert {:error, {:oversized_declared, 1001, @max}, _g} = FrameGuard.feed(g, header)
    end

    test "an exact-boundary declared payload is accepted; one byte more is refused" do
      g = FrameGuard.new(max_message: @max)
      ok_header = <<1::1, 0::3, 1::4, 0::1, 126::7, 1000::16-unsigned>>
      assert {:ok, _g} = FrameGuard.feed(g, ok_header <> String.duplicate("a", 1000))

      g = FrameGuard.new(max_message: @max)
      big = <<1::1, 0::3, 1::4, 0::1, 126::7, 1001::16-unsigned>>
      assert {:error, {:oversized_declared, 1001, @max}, _} = FrameGuard.feed(g, big <> "x")
    end

    test "a fragmented aggregate is refused at the crossing continuation" do
      g = FrameGuard.new(max_message: @max)

      first = <<0::1, 0::3, 1::4, 0::1, 125::7, String.duplicate("a", 125)::binary>>
      assert {:ok, g} = FrameGuard.feed(g, first)
      assert FrameGuard.message_bytes(g) == 125

      # 901 more would cross; the guard refuses on the continuation's
      # declared length, before its body needs to arrive.
      cont = <<0::1, 0::3, 0::4, 0::1, 126::7, 901::16-unsigned>>
      assert {:error, {:oversized_message, 1026, @max}, _} = FrameGuard.feed(g, cont)
    end

    test "a fragmented aggregate that lands exactly on max is accepted" do
      g = FrameGuard.new(max_message: @max)

      first = <<0::1, 0::3, 1::4, 0::1, 125::7, String.duplicate("a", 125)::binary>>
      assert {:ok, g} = FrameGuard.feed(g, first)
      assert FrameGuard.message_bytes(g) == 125

      cont = <<1::1, 0::3, 0::4, 0::1, 126::7, 875::16-unsigned>>
      assert {:ok, _g} = FrameGuard.feed(g, cont <> String.duplicate("b", 875))
    end

    test "control frames over 125 bytes or fragmented are refused" do
      g = FrameGuard.new(max_message: @max)

      oversized_ping = <<1::1, 0::3, 9::4, 0::1, 126::7, 200::16-unsigned>>
      assert {:error, {:oversized_control, 200}, _} = FrameGuard.feed(g, oversized_ping <> "p")

      fragmented_ping = <<0::1, 0::3, 9::4, 0::1, 1::7, "x">>
      assert {:error, :fragmented_control, _} = FrameGuard.feed(g, fragmented_ping)
    end

    test "a partial header is retained across feeds, byte by byte" do
      g = FrameGuard.new(max_message: @max)

      frame =
        <<1::1, 0::3, 1::4, 0::1, 126::7, 300::16-unsigned, String.duplicate("c", 300)::binary>>

      _g =
        for <<b <- frame>>, reduce: g do
          acc ->
            assert {:ok, g} = FrameGuard.feed(acc, <<b>>)
            g
        end
    end
  end

  # -- Ws over a real socket ---------------------------------------------------

  defp connect_ws!(port, opts \\ []) do
    assert {:ok, ws, []} = Ws.connect("127.0.0.1", port, "/", opts ++ [max_message: 65_536])
    ws
  end

  # TCP may deliver the scripted frames split across reads; a quiet read
  # is not a verdict. Read until the transport produces one (or the
  # deadline passes) so the assertion judges settled state, not delivery
  # order.
  defp recv_until(ws, deadline_ms \\ 2_000) do
    case Ws.recv_frames(ws, 250) do
      {:ok, ws, []} ->
        if deadline_ms <= 0, do: {:timeout, ws}, else: recv_until(ws, deadline_ms - 250)

      settled ->
        settled
    end
  end

  # For burst cases the property under test IS the eventual verdict
  # (error or close): intermediate non-empty batches are expected and
  # discarded — loopback packetization is not an oracle.
  defp recv_until_verdict(ws, deadline_ms \\ 3_000) do
    case Ws.recv_frames(ws, 250) do
      {:ok, ws, _intermediate_batch} ->
        if deadline_ms <= 0,
          do: {:no_verdict, ws},
          else: recv_until_verdict(ws, deadline_ms - 250)

      verdict ->
        verdict
    end
  end

  test "a declared-oversized frame is refused and releases the transport" do
    {:ok, peer, port} = SynthPeer.start()

    on_exit(fn ->
      SynthPeer.stop(peer)
    end)

    ws = connect_ws!(port)
    :ok = SynthPeer.send_frame(peer, 1, true, String.duplicate("x", 65_537))

    assert {:error, ws, {:guard, {:oversized_declared, 65_537, 65_536}}} =
             Ws.recv_frames(ws, 2_000)

    assert Ws.socket(ws) == nil
    assert {:error, _, :closed_transport} = Ws.recv_frames(ws, 100)
  end

  test "a fragmented message crossing the bound is refused (and the exact bound passes)" do
    {:ok, peer, port} = SynthPeer.start()

    on_exit(fn ->
      SynthPeer.stop(peer)
    end)

    ws = connect_ws!(port)
    :ok = SynthPeer.send_frame(peer, 1, false, String.duplicate("x", 60_000))
    :ok = SynthPeer.send_frame(peer, 0, true, String.duplicate("y", 10_000))

    assert {:error, ws, {:guard, {:oversized_message, 70_000, 65_536}}} =
             recv_until(ws)

    assert Ws.socket(ws) == nil

    # Nearby valid control: the same fragmentation landing exactly on
    # the bound is delivered as one text frame.
    {:ok, peer2, port2} = SynthPeer.start()

    on_exit(fn ->
      SynthPeer.stop(peer2)
    end)

    ws2 = connect_ws!(port2)
    :ok = SynthPeer.send_frame(peer2, 1, false, String.duplicate("x", 60_000))
    :ok = SynthPeer.send_frame(peer2, 0, true, String.duplicate("y", 5_536))

    assert {:ok, _ws2, [{:text, text}]} = recv_until(ws2)
    assert byte_size(text) == 65_536
  end

  test "malformed control frames (oversized, fragmented) are refused" do
    {:ok, peer, port} = SynthPeer.start()

    on_exit(fn ->
      SynthPeer.stop(peer)
    end)

    ws = connect_ws!(port)
    :ok = SynthPeer.send_frame(peer, 9, true, String.duplicate("p", 200))
    assert {:error, ws, {:guard, {:oversized_control, 200}}} = Ws.recv_frames(ws, 2_000)
    assert Ws.socket(ws) == nil

    {:ok, peer2, port2} = SynthPeer.start()

    on_exit(fn ->
      SynthPeer.stop(peer2)
    end)

    ws2 = connect_ws!(port2)
    :ok = SynthPeer.send_frame(peer2, 9, false, "p")
    assert {:error, ws2, {:guard, :fragmented_control}} = Ws.recv_frames(ws2, 2_000)
    assert Ws.socket(ws2) == nil
  end

  test "a valid ping is answered with one pong (nearby valid control)" do
    {:ok, peer, port} = SynthPeer.start()

    on_exit(fn ->
      SynthPeer.stop(peer)
    end)

    ws = connect_ws!(port)
    :ok = SynthPeer.send_frame(peer, 9, true, String.duplicate("p", 125))

    assert {:ok, _ws, [{:ping, _body}]} = Ws.recv_frames(ws, 2_000)
    assert {:ok, {0xA, true, pong}} = SynthPeer.expect_frame(peer, 2_000)
    assert byte_size(pong) == 125
  end

  test "a ping burst beyond the per-batch cap fails the read and closes the transport" do
    {:ok, peer, port} = SynthPeer.start()

    on_exit(fn ->
      SynthPeer.stop(peer)
    end)

    ws = connect_ws!(port)

    ping = <<1::1, 0::3, 9::4, 0::1, 1::7, "x">>
    burst = List.duplicate(ping, 300) |> IO.iodata_to_binary()
    :ok = SynthPeer.send_raw(peer, burst)

    # A single peer write need not land as a single receive batch:
    # read until the transport produces a VERDICT (error or close) —
    # any batch over the cap must fail the read and close.
    assert {:error, ws, {:ping_flood, _n, 32}} = recv_until_verdict(ws)
    assert Ws.socket(ws) == nil
  end

  test "a peer close frame is surfaced and echoed exactly once" do
    {:ok, peer, port} = SynthPeer.start()

    on_exit(fn ->
      SynthPeer.stop(peer)
    end)

    ws = connect_ws!(port)
    :ok = SynthPeer.send_frame(peer, 8, true, <<1_000::16, "bye">>)

    assert {:ok, _ws, [{:close, 1000, "bye"}]} = Ws.recv_frames(ws, 2_000)
    # Our echo of the close reaches the peer...
    assert {:ok, {8, true, _echo}} = SynthPeer.expect_frame(peer, 2_000)
    # ...and close/2 always releases the owned socket.
    assert :ok = Ws.close(ws)
  end
end
