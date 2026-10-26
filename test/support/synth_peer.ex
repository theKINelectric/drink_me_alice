defmodule DrinkMeAlice.SynthPeer do
  @moduledoc """
  Owned synthetic WebSocket peer (server side) on 127.0.0.1 for framing
  probes. Spawned (not linked) and monitored; the caller transfers listener
  ownership during a startup handshake, and the peer exits if the owner
  dies. Every wait is finite; `stop/1` closes both sockets and reports the
  reap honestly.

  Protocol duties: accept one connection, read the client's HTTP upgrade
  request (bounded #{8_192}-byte header section, 2 s deadline), reply 101
  with a correct `Sec-WebSocket-Accept`, then accept scripted sends.
  Incoming client frames (masked per RFC 6455) are parsed and reported to
  the owner via `expect_frame/2`; leftover bytes across expects are kept in
  a bounded `pending` buffer.

  Owner API:

    * `start/0` -> `{:ok, peer, port}` — peer owns the listener and accepts
      immediately (bounded #{5_000} ms); the caller is the lifecycle owner
    * `send_raw(peer, iodata)` — bytes verbatim on the wire
    * `send_frame(peer, opcode, fin?, payload)` — one well-formed frame
    * `expect_frame(peer, timeout)` — next parsed client frame
      `{opcode, fin?, payload}`; a timed-out expect should be followed by
      `stop/1` since the request may still be queued peer-side
    * `close_tcp(peer)` — close the client socket without a close frame
    * `stop(peer)` — close everything; returns `:ok | {:error, :unreaped}`
    * `forward_to(peer, pid)` — enter forward mode: every parsed client
      frame is pushed to `pid` as `{:synth_frame, {opcode, fin?, payload}}`;
      peer close/error reports `:synth_closed` / `{:synth_error, reason}`.
      Unlike `expect_frame/2`, a quiet socket is NOT fatal in this mode;
      `send_frame`/`close_tcp`/`stop` stay responsive between ~20 ms
      read slices. This is the mode `FakeSurreal` uses — `expect_frame`
      kills the peer on timeout by design, so it cannot serve as a poll.
  """

  @max_upgrade_header 8_192
  @max_inbound 2_097_216
  # Outbound sends are bounded but large enough for a small oversized-header
  # fault fixture (a header declaring >1 MiB plus a little body).
  @max_outbound 1_179_648
  @accept_timeout 5_000
  @upgrade_timeout 2_000
  @idle_timeout 60_000
  @send_timeout 2_000
  @fwd_slice_ms 20

  defstruct [:listen, :sock, :owner, :owner_ref, :fwd, pending: <<>>]

  @spec start() :: {:ok, pid(), :inet.port_number()} | {:error, term()}
  def start() do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, recbuf: 262_144])

    {:ok, {_, port}} = :inet.sockname(listen)
    ref = make_ref()
    peer = spawn(fn -> boot(ref) end)

    case :gen_tcp.controlling_process(listen, peer) do
      :ok ->
        send(peer, {:take, self(), ref, listen})

        receive do
          {:peer_ready, ^ref} -> {:ok, peer, port}
        after
          2_000 ->
            safe_kill(peer)
            :gen_tcp.close(listen)
            {:error, :peer_boot_timeout}
        end

      {:error, reason} ->
        safe_kill(peer)
        :gen_tcp.close(listen)
        {:error, {:listener_transfer, reason}}
    end
  end

  @spec send_raw(pid(), iodata()) :: :ok | {:error, :send_overrun}
  def send_raw(peer, data) do
    bin = IO.iodata_to_binary(data)

    if byte_size(bin) > @max_outbound do
      {:error, :send_overrun}
    else
      send(peer, {:send_raw, bin})
      :ok
    end
  end

  @spec send_frame(pid(), non_neg_integer(), boolean(), binary()) ::
          :ok | {:error, :send_overrun}
  def send_frame(peer, opcode, fin?, payload)
      when is_binary(payload) and opcode in 0..15 do
    if byte_size(payload) > @max_outbound - 10 do
      {:error, :send_overrun}
    else
      send(peer, {:send_frame, opcode, fin?, payload})
      :ok
    end
  end

  @doc """
  Wait for the next parsed client frame `{opcode, fin?, payload}`.

  Replies: `{:ok, frame}` | `:timeout` | `:closed` | `{:error, reason}`.
  The peer enforces the same deadline; on `:timeout` the request may remain
  queued — use per-case peers and `stop/1` afterwards.
  """
  @spec expect_frame(pid(), pos_integer()) ::
          {:ok, tuple()} | :timeout | :closed | {:error, term()}
  def expect_frame(peer, timeout) when is_integer(timeout) and timeout > 0 do
    ref = make_ref()
    deadline = System.monotonic_time(:millisecond) + timeout
    send(peer, {:expect, self(), ref, deadline})

    receive do
      {:peer_frame, ^ref, frame} -> {:ok, frame}
      {:peer_closed, ^ref} -> :closed
      {:peer_error, ^ref, reason} -> {:error, reason}
    after
      timeout -> :timeout
    end
  end

  @spec close_tcp(pid()) :: :ok
  def close_tcp(peer) do
    send(peer, :close_tcp)
    :ok
  end

  @spec forward_to(pid(), pid()) :: :ok
  def forward_to(peer, pid) when is_pid(pid) do
    send(peer, {:forward, pid})
    :ok
  end

  @spec stop(pid()) :: :ok | {:error, :unreaped}
  def stop(peer) do
    ref = Process.monitor(peer)
    send(peer, :stop)

    receive do
      {:DOWN, ^ref, :process, ^peer, _} ->
        :ok
    after
      2_000 ->
        Process.exit(peer, :kill)

        receive do
          {:DOWN, ^ref, :process, ^peer, _} -> :ok
        after
          2_000 -> {:error, :unreaped}
        end
    end
  end

  defp safe_kill(peer) do
    if Process.alive?(peer), do: Process.exit(peer, :kill)
    :ok
  end

  # -- internal ---------------------------------------------------------------

  defp boot(ref) do
    receive do
      {:take, owner, ^ref, listen} ->
        owner_ref = Process.monitor(owner)
        state = %__MODULE__{listen: listen, owner: owner, owner_ref: owner_ref}
        send(owner, {:peer_ready, ref})
        accept(state)
    after
      3_000 -> :ok
    end
  end

  defp accept(state) do
    case :gen_tcp.accept(state.listen, @accept_timeout) do
      {:ok, sock} ->
        :inet.setopts(sock, send_timeout: @send_timeout, send_timeout_close: true)
        state = %{state | sock: sock}

        case read_upgrade(state) do
          {:ok, state} -> loop(state)
          {:error, _} -> cleanup(state)
        end

      {:error, _} ->
        cleanup(state)
    end
  end

  defp read_upgrade(state) do
    deadline = System.monotonic_time(:millisecond) + @upgrade_timeout

    case read_headers(state.sock, <<>>, deadline) do
      {:ok, request, leftover} ->
        key = header(request, "sec-websocket-key")

        if is_nil(key) do
          {:error, :no_key}
        else
          accept_key =
            :crypto.hash(:sha, key <> "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
            |> Base.encode64()

          reply =
            "HTTP/1.1 101 Switching Protocols\r\n" <>
              "upgrade: websocket\r\n" <>
              "connection: upgrade\r\n" <>
              "sec-websocket-accept: " <> accept_key <> "\r\n\r\n"

          case :gen_tcp.send(state.sock, reply) do
            :ok -> {:ok, %{state | pending: leftover}}
            {:error, r} -> {:error, r}
          end
        end

      {:error, r} ->
        {:error, r}
    end
  end

  defp read_headers(sock, acc, deadline) do
    cond do
      byte_size(acc) > @max_upgrade_header ->
        {:error, :header_overrun}

      String.contains?(acc, "\r\n\r\n") ->
        [head, rest] = :binary.split(acc, "\r\n\r\n")
        {:ok, head, rest}

      true ->
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining <= 0 do
          {:error, :header_timeout}
        else
          case :gen_tcp.recv(sock, 0, remaining) do
            {:ok, d} -> read_headers(sock, acc <> d, deadline)
            {:error, r} -> {:error, r}
          end
        end
    end
  end

  defp header(request, name) do
    request
    |> :binary.split("\r\n", [:global])
    |> Enum.find_value(fn line ->
      case :binary.split(line, ":") do
        [k, v] -> if String.downcase(String.trim(k)) == name, do: String.trim(v)
        _ -> nil
      end
    end)
  end

  defp loop(state) do
    receive do
      {:DOWN, ref, :process, pid, _r} when ref == state.owner_ref and pid == state.owner ->
        cleanup(state)

      {:send_raw, data} ->
        case safe_send(state, data) do
          :ok -> loop(state)
          :error -> cleanup(state)
        end

      {:send_frame, opcode, fin?, payload} ->
        case safe_send(state, encode_frame(opcode, fin?, payload)) do
          :ok -> loop(state)
          :error -> cleanup(state)
        end

      {:expect, pid, ref, deadline} ->
        if System.monotonic_time(:millisecond) >= deadline do
          # Stale expect: deadline already passed — drop silently so no
          # late reply can land in a later caller's mailbox.
          loop(state)
        else
          case read_client_frame(state, deadline) do
            {:ok, frame, state} ->
              send(pid, {:peer_frame, ref, frame})
              loop(state)

            {:closed, state} ->
              send(pid, {:peer_closed, ref})
              cleanup(state)

            {:error, r, state} ->
              send(pid, {:peer_error, ref, r})
              cleanup(state)
          end
        end

      {:forward, pid} ->
        forward_loop(%{state | fwd: pid})

      :close_tcp ->
        cleanup(state)

      :stop ->
        cleanup(state)
    after
      @idle_timeout -> cleanup(state)
    end
  end

  # Forward mode: read slices push every parsed client frame to `fwd`.
  # A quiet slice is not fatal — commands are serviced between slices.
  defp forward_loop(state) do
    deadline = System.monotonic_time(:millisecond) + @fwd_slice_ms

    case read_client_frame(state, deadline) do
      {:ok, frame, state} ->
        send(state.fwd, {:synth_frame, frame})
        forward_loop(state)

      {:error, r, state} when r in [:expect_timeout, :timeout] ->
        # Quiet slice — service queued commands, keep reading. (gen_tcp
        # reports a deadline hit as {:error, :timeout}; read_client_frame
        # reports a stale deadline as :expect_timeout. Neither is fatal
        # in forward mode.)
        case forward_commands(state) do
          {:cont, state} -> forward_loop(state)
          :halt -> :ok
        end

      {:closed, state} ->
        send(state.fwd, :synth_closed)
        cleanup(state)

      {:error, r, state} ->
        send(state.fwd, {:synth_error, r})
        cleanup(state)
    end
  end

  defp forward_commands(state) do
    receive do
      {:DOWN, ref, :process, pid, _r} when ref == state.owner_ref and pid == state.owner ->
        cleanup(state)
        :halt

      {:send_raw, data} ->
        case safe_send(state, data) do
          :ok -> {:cont, state}
          :error -> cleanup_and_halt(state)
        end

      {:send_frame, opcode, fin?, payload} ->
        case safe_send(state, encode_frame(opcode, fin?, payload)) do
          :ok -> {:cont, state}
          :error -> cleanup_and_halt(state)
        end

      :close_tcp ->
        cleanup(state)
        :halt

      :stop ->
        cleanup(state)
        :halt
    after
      0 -> {:cont, state}
    end
  end

  defp cleanup_and_halt(state) do
    cleanup(state)
    :halt
  end

  defp read_client_frame(state, deadline) do
    # Bound accumulated bytes before decoding so a complete oversized
    # frame is rejected rather than unmasked and returned.
    if byte_size(state.pending) > @max_inbound do
      {:error, :inbound_overrun, state}
    else
      case parse_client_frame(state.pending) do
        {:ok, frame, rest} ->
          {:ok, frame, %{state | pending: rest}}

        {:error, r} ->
          {:error, r, state}

        :incomplete ->
          remaining = deadline - System.monotonic_time(:millisecond)

          if remaining <= 0 do
            {:error, :expect_timeout, state}
          else
            case :gen_tcp.recv(state.sock, 0, remaining) do
              {:ok, d} ->
                if byte_size(state.pending) + byte_size(d) > @max_inbound do
                  {:error, :inbound_overrun, state}
                else
                  read_client_frame(%{state | pending: state.pending <> d}, deadline)
                end

              {:error, :closed} ->
                {:closed, state}

              {:error, r} ->
                {:error, r, state}
            end
          end
      end
    end
  end

  # Client frames are masked. Returns {:ok, {opcode, fin, payload}, rest}.
  defp parse_client_frame(<<fin::1, _r::3, op::4, 1::1, len::7, rest::binary>>) do
    with {:ok, declared, rest} <- ext_len(len, rest),
         true <- declared <= @max_inbound || {:declared_overrun, declared},
         {:ok, mask, rest2} <- take(rest, 4),
         true <- byte_size(rest2) >= declared do
      <<payload::binary-size(^declared), tail::binary>> = rest2
      {:ok, {op, fin == 1, unmask(payload, mask)}, tail}
    else
      {:declared_overrun, _} -> {:error, :declared_overrun}
      _ -> :incomplete
    end
  end

  defp parse_client_frame(<<_::1, _::3, _::4, 0::1, _::7, _::binary>>), do: {:error, :unmasked}
  defp parse_client_frame(_), do: :incomplete

  defp ext_len(len, rest) when len in 0..125, do: {:ok, len, rest}
  defp ext_len(126, <<l::unsigned-16, rest::binary>>), do: {:ok, l, rest}
  defp ext_len(126, _), do: :incomplete
  defp ext_len(127, <<l::unsigned-64, rest::binary>>), do: {:ok, l, rest}
  defp ext_len(127, _), do: :incomplete

  defp take(b, n) when byte_size(b) >= n do
    <<h::binary-size(^n), t::binary>> = b
    {:ok, h, t}
  end

  defp take(_, _), do: :incomplete

  defp unmask(payload, mask), do: unmask(payload, mask, 0, <<>>)
  defp unmask(<<>>, _mask, _i, acc), do: acc

  defp unmask(<<b, rest::binary>>, mask, i, acc) do
    unmask(rest, mask, i + 1, <<acc::binary, Bitwise.bxor(b, :binary.at(mask, rem(i, 4)))>>)
  end

  # Server->client frames are unmasked.
  defp encode_frame(opcode, fin?, payload) do
    len = byte_size(payload)
    fin = if fin?, do: 1, else: 0

    ext =
      cond do
        len <= 125 -> <<len::7>>
        len <= 65_535 -> <<126::7, len::unsigned-16>>
        true -> <<127::7, len::unsigned-64>>
      end

    <<fin::1, 0::3, opcode::4, 0::1, ext::bitstring, payload::binary>>
  end

  defp safe_send(state, data) do
    case :gen_tcp.send(state.sock, data) do
      :ok -> :ok
      {:error, _} -> :error
    end
  end

  defp cleanup(state) do
    if state.sock, do: :gen_tcp.close(state.sock)
    if state.listen, do: :gen_tcp.close(state.listen)
    :ok
  end
end
