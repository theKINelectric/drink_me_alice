defmodule DrinkMeAlice.Ws do
  @moduledoc """
  Passive, bounded WebSocket transport over Mint 1.10.1 /
  mint_web_socket 1.0.6 — production adaptation of the accepted BF-02A
  `SurrealTransport.Ws` probe.

  - HTTP/1 only (`protocols: [:http1]`), `mode: :passive` on connect and
    on `Mint.WebSocket.new/5`; inbound data arrives only through bounded
    `Mint.WebSocket.recv/3` calls. The BEAM mailbox is never an unbounded
    buffer for socket data.
  - No WebSocket extensions are proposed, so permessage-deflate cannot be
    negotiated — compression is disabled by construction.
  - `DrinkMeAlice.FrameGuard` walks raw bytes ahead of
    `Mint.WebSocket.decode/2` and refuses declared or aggregate payloads
    over `:max_message` (1 MiB); Mint-side `buffer`+`fragment` retention
    is separately tripwired at `:max_retained`.
  - Caller-driven ping/pong: pings are counted, bounded per batch, and
    answered with one batched send.
  - All waits are bounded: connect/upgrade deadlines, `send_timeout`,
    `max_header_list_size` on the upgrade response, per-`recv_frames/2`
    frame/byte/ping bounds.
  - Error tuples are `{:error, t(), reason}` / `{:closed, t(), frames}` /
    `{:timeout, t()}`; reasons are sanitized — never Mint structs, frame
    payloads, headers, or auth material.
  - Fatal input paths close the owned socket at the recv boundary and
    return `conn: nil`, so `close/2`, `recv_frames/2` and `send_frame/2`
    stay safe and idempotent.

  Text payloads are decoded through `DrinkMeAlice.Json` (the bounded
  Surreal-dialect profile) by `classify/1` or by the owning
  `DrinkMeAlice.Conn` — never by unbounded `:json.decode`.
  Request correlation is owned by `Conn`; this module only moves frames.
  """

  alias DrinkMeAlice.{FrameGuard, Json}

  @max_frames_per_recv 512
  @max_pings_per_recv 32
  @max_upgrade_header 16_384
  @recvbuf 131_072
  @connect_timeout 2_000
  @upgrade_timeout 2_000
  @send_timeout 2_000

  defstruct conn: nil,
            ws: nil,
            ref: nil,
            guard: nil,
            max_message: 1_048_576,
            max_retained: 2_097_216,
            retained_peak: 0,
            auto_pong: true,
            close_attempted: false

  @type t :: %__MODULE__{}

  @type classified ::
          {:text, binary()}
          | {:binary, binary()}
          | {:notification, map(), non_neg_integer()}
          | {:ping, binary()}
          | {:pong, binary()}
          | {:close, non_neg_integer() | nil, binary() | nil}
          | {:malformed, term()}

  # -- connection ------------------------------------------------------------

  @doc """
  Connect over cleartext HTTP/1 and upgrade `path` to a WebSocket.

  Returns `{:ok, state, early_frames}` — complete frames whose bytes
  arrived coalesced with the 101 response. They pass through the same
  guard/ping/close handling as later traffic.

  Options: `:connect_timeout`, `:upgrade_timeout`, `:send_timeout`,
  `:max_message`, `:protocols` (default `["json"]`), `:headers`,
  `:auto_pong`.
  """
  @spec connect(
          :inet.hostname() | :inet.ip4_address(),
          :inet.port_number(),
          String.t(),
          keyword()
        ) ::
          {:ok, t(), [classified()]} | {:error, term()}
  def connect(host, port, path, opts \\ []) do
    protocols =
      case Keyword.get(opts, :protocols, ["json"]) do
        [] -> []
        ps -> [{"sec-websocket-protocol", Enum.join(ps, ", ")}]
      end

    transport_opts = [
      active: false,
      mode: :binary,
      packet: :raw,
      recbuf: @recvbuf,
      sndbuf: @recvbuf,
      nodelay: true,
      send_timeout: Keyword.get(opts, :send_timeout, @send_timeout),
      send_timeout_close: true
    ]

    address = normalize_host(host)

    case Mint.HTTP.connect(:http, address, port,
           mode: :passive,
           protocols: [:http1],
           transport_opts: transport_opts,
           max_header_list_size: @max_upgrade_header,
           timeout: Keyword.get(opts, :connect_timeout, @connect_timeout)
         ) do
      {:ok, conn} -> do_upgrade(conn, path, protocols, opts)
      {:error, reason} -> {:error, sanitize(reason)}
    end
  end

  defp normalize_host(host) when is_binary(host), do: host
  defp normalize_host(host) when is_tuple(host), do: host |> :inet.ntoa() |> List.to_string()
  defp normalize_host(host) when is_atom(host), do: Atom.to_string(host)

  defp do_upgrade(conn, path, protocols, opts) do
    headers = protocols ++ Keyword.get(opts, :headers, [])
    upgrade_timeout = Keyword.get(opts, :upgrade_timeout, @upgrade_timeout)

    with {:ok, conn, ref} <- Mint.WebSocket.upgrade(:ws, conn, path, headers, extensions: []),
         {:ok, conn, status, resp_headers, leftover} <- await_upgrade(conn, ref, upgrade_timeout),
         {:ok, conn, ws} <- Mint.WebSocket.new(conn, ref, status, resp_headers, mode: :passive) do
      max_message = Keyword.get(opts, :max_message, 1_048_576)

      state = %__MODULE__{
        conn: conn,
        ws: ws,
        ref: ref,
        max_message: max_message,
        max_retained: 2 * max_message + 64,
        guard: FrameGuard.new(max_message: max_message),
        auto_pong: Keyword.get(opts, :auto_pong, true)
      }

      if leftover == <<>> do
        {:ok, state, []}
      else
        case feed(state, leftover) do
          {:ok, state, frames} ->
            case handle_frames(state, frames) do
              {:ok, state, frames} -> {:ok, state, frames}
              {:error, state, reason} -> shutdown(state, reason)
            end

          {:error, state, reason} ->
            shutdown(state, reason)
        end
      end
    else
      {:error, conn, reason} ->
        _ = safe_close_transport(conn)
        {:error, sanitize(reason)}

      {:error, reason} ->
        _ = safe_close_transport(conn)
        {:error, sanitize(reason)}
    end
  end

  # Passive-mode upgrade: read the HTTP/1 101 response via recv/3.
  # Bounded by deadline, Mint's max_header_list_size (16 KiB) on the
  # header section, and a recvbuf-sized cap on coalesced post-101 bytes.
  defp await_upgrade(conn, ref, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    collect_upgrade(conn, ref, deadline, nil, [], [])
  end

  defp collect_upgrade(conn, ref, deadline, status, headers, leftover) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:error, :upgrade_timeout}
    else
      case Mint.HTTP.recv(conn, 0, remaining) do
        {:ok, conn, responses} ->
          case Enum.reduce_while(
                 responses,
                 {:cont, status, headers, leftover},
                 &upgrade_step(ref, &1, &2)
               ) do
            {:done, status, headers, leftover} ->
              {:ok, conn, status, headers, IO.iodata_to_binary(Enum.reverse(leftover))}

            {:cont, status, headers, leftover} ->
              if IO.iodata_length(leftover) > @recvbuf do
                {:error, conn, :upgrade_leftover_exceeded}
              else
                collect_upgrade(conn, ref, deadline, status, headers, leftover)
              end

            {:error, reason} ->
              {:error, conn, reason}
          end

        {:error, conn, reason, _responses} ->
          {:error, conn, reason}
      end
    end
  end

  defp upgrade_step(ref, response, {:cont, status, headers, leftover}) do
    case response do
      {:status, ^ref, s} -> {:cont, {:cont, s, headers, leftover}}
      {:headers, ^ref, h} -> {:cont, {:cont, status, h, leftover}}
      {:data, ^ref, d} -> {:cont, {:cont, status, headers, [d | leftover]}}
      {:done, ^ref} -> {:halt, {:done, status, headers, leftover}}
      {:closed, ^ref} -> {:halt, {:error, :closed_during_upgrade}}
      {:error, ^ref, r} -> {:halt, {:error, r}}
      _other -> {:cont, {:cont, status, headers, leftover}}
    end
  end

  # -- frames ----------------------------------------------------------------

  @doc "Bytes currently retained inside Mint.WebSocket (buffer + fragment)."
  @spec retained_bytes(t()) :: non_neg_integer()
  def retained_bytes(%__MODULE__{ws: nil}), do: 0
  def retained_bytes(%__MODULE__{ws: ws}), do: ws_retained(ws)

  @doc "Payload bytes accumulated in the guard's open fragmented message."
  @spec guard_message_bytes(t()) :: non_neg_integer()
  def guard_message_bytes(%__MODULE__{guard: g}), do: FrameGuard.message_bytes(g)

  defp ws_retained(%Mint.WebSocket{} = ws) do
    byte_size(ws.buffer) + fragment_bytes(ws.fragment)
  end

  # The fragment slot holds {:text|:binary|:continuation, reserved, mask,
  # data, fin?}; data is element 3.
  defp fragment_bytes(nil), do: 0

  defp fragment_bytes(frag) when is_tuple(frag) and tuple_size(frag) >= 4 do
    case elem(frag, 3) do
      data when is_binary(data) -> byte_size(data)
      _ -> 0
    end
  end

  defp fragment_bytes(_), do: 0

  @doc """
  Feed raw inbound bytes through the framing guard, then Mint decode.

  Frames in the returned list may include `{:notification, body,
  raw_byte_size}` (SurrealDB live-query notifications classified inline —
  decoded through the bounded `Json` profile, never stored as terms
  beyond the raw byte size) and `{:malformed, reason}` entries.
  """
  @spec feed(t(), binary()) :: {:ok, t(), [classified()]} | {:error, t(), term()}
  def feed(%__MODULE__{} = state, bytes) when is_binary(bytes) do
    case FrameGuard.feed(state.guard, bytes) do
      {:ok, guard} ->
        state = %{state | guard: guard}

        case Mint.WebSocket.decode(state.ws, bytes) do
          {:ok, ws, frames} ->
            retained = ws_retained(ws)
            state = %{state | ws: ws, retained_peak: max(state.retained_peak, retained)}

            if retained > state.max_retained do
              {:error, state, {:retained_overrun, retained, state.max_retained}}
            else
              {:ok, state, Enum.map(frames, &classify/1)}
            end

          {:error, ws, reason} ->
            {:error, %{state | ws: ws}, {:decode, sanitize_frame_error(reason)}}
        end

      {:error, reason, guard} ->
        {:error, %{state | guard: guard}, {:guard, reason}}
    end
  end

  defp classify({:text, text}) do
    case Json.decode(text) do
      {:ok, %{"result" => %{"id" => _live, "action" => action, "result" => _} = body} = decoded}
      when is_binary(action) and not is_map_key(decoded, "id") ->
        {:notification, body, byte_size(text)}

      {:ok, %{"method" => "notification", "params" => params}} ->
        {:notification, params, byte_size(text)}

      _ ->
        {:text, text}
    end
  end

  defp classify({:error, reason}), do: {:malformed, sanitize_frame_error(reason)}
  defp classify(frame), do: frame

  @doc """
  One bounded socket read + guard + decode. Returns `{:ok, state, frames}`,
  `{:timeout, state}`, `{:closed, state, frames}` or `{:error, state, r}`.

  Every decoded frame is counted against a per-call cap before any ping
  is answered; pings beyond the batch cap fail the call. Server pings are
  answered with one batched send. A peer close frame is echoed at most
  once.
  """
  @spec recv_frames(t(), timeout()) ::
          {:ok, t(), [classified()]}
          | {:timeout, t()}
          | {:closed, t(), [classified()]}
          | {:error, t(), term()}
  def recv_frames(state, timeout \\ 2_000)

  def recv_frames(%__MODULE__{conn: nil} = state, _timeout) do
    {:error, state, :closed_transport}
  end

  def recv_frames(%__MODULE__{} = state, timeout) do
    case Mint.WebSocket.recv(state.conn, 0, timeout) do
      {:ok, conn, responses} ->
        state = %{state | conn: conn}

        case fold_responses(state, responses) do
          {:ok, state, frames} ->
            case handle_frames(state, frames) do
              {:ok, state, frames} -> {:ok, state, frames}
              {:error, state, reason} -> fatal(state, reason)
            end

          {:closed, state, frames} ->
            {:closed, state, frames}

          {:error, state, reason} ->
            fatal(state, reason)
        end

      {:error, conn, :timeout, _r} ->
        {:timeout, %{state | conn: conn}}

      {:error, conn, :closed, _r} ->
        {:closed, %{state | conn: conn}, []}

      {:error, conn, reason, _r} ->
        fatal(%{state | conn: conn}, sanitize(reason))
    end
  end

  # Every fatal input path closes the owned transport and nils the conn.
  defp fatal(state, reason) do
    _ = safe_close_transport(state.conn)
    {:error, %{state | conn: nil}, reason}
  end

  defp fold_responses(state, responses) do
    Enum.reduce_while(responses, {:ok, state, []}, fn
      {:data, ref, data}, {:ok, st, acc} when ref == st.ref ->
        case feed(st, data) do
          {:ok, st, frames} -> {:cont, {:ok, st, acc ++ frames}}
          {:error, st, reason} -> {:halt, {:error, st, reason}}
        end

      {:closed, ref}, {:ok, st, acc} when ref == st.ref ->
        {:halt, {:closed, st, acc}}

      {:error, ref, reason}, {:ok, st, _acc} when ref == st.ref ->
        {:halt, {:error, st, sanitize_frame_error(reason)}}

      _other, acc_tuple ->
        {:cont, acc_tuple}
    end)
  end

  defp handle_frames(state, frames) do
    malformed = Enum.find(frames, &match?({:malformed, _}, &1))

    cond do
      malformed != nil ->
        {:error, state, {:malformed_frame, elem(malformed, 1)}}

      length(frames) > @max_frames_per_recv ->
        {:error, state, {:frame_batch_exceeded, length(frames), @max_frames_per_recv}}

      true ->
        pings = for {:ping, body} <- frames, do: body

        cond do
          length(pings) > @max_pings_per_recv ->
            {:error, state, {:ping_flood, length(pings), @max_pings_per_recv}}

          pings != [] and state.auto_pong ->
            case send_pongs(state, pings) do
              {:ok, state} -> echo_close(state, frames)
              {:error, state, reason} -> {:error, state, reason}
            end

          true ->
            echo_close(state, frames)
        end
    end
  end

  defp send_pongs(state, bodies) do
    Enum.reduce_while(bodies, {:ok, state, []}, fn body, {:ok, st, iodata} ->
      case Mint.WebSocket.encode(st.ws, {:pong, body}) do
        {:ok, ws, data} ->
          {:cont, {:ok, %{st | ws: ws}, [iodata | data]}}

        {:error, ws, reason} ->
          {:halt, {:error, %{st | ws: ws}, {:encode, sanitize_frame_error(reason)}}}
      end
    end)
    |> case do
      {:ok, st, iodata} ->
        case Mint.WebSocket.stream_request_body(st.conn, st.ref, iodata) do
          {:ok, conn} -> {:ok, %{st | conn: conn}}
          {:error, conn, reason} -> {:error, %{st | conn: conn}, sanitize(reason)}
        end

      {:error, st, reason} ->
        {:error, st, reason}
    end
  end

  defp echo_close(state, frames) do
    if state.close_attempted or not Enum.any?(frames, &match?({:close, _, _}, &1)) do
      {:ok, state, frames}
    else
      {:close, code, reason} = Enum.find(frames, &match?({:close, _, _}, &1))

      case send_frame(state, {:close, code || 1_000, reason || ""}) do
        {:ok, state} -> {:ok, %{state | close_attempted: true}, frames}
        {:error, state, r} -> {:error, state, r}
      end
    end
  end

  @doc "Encode and send one frame."
  @spec send_frame(t(), Mint.WebSocket.frame() | Mint.WebSocket.shorthand_frame()) ::
          {:ok, t()} | {:error, t(), term()}
  def send_frame(%__MODULE__{conn: nil} = state, _frame) do
    {:error, state, :closed_transport}
  end

  def send_frame(%__MODULE__{} = state, frame) do
    case Mint.WebSocket.encode(state.ws, frame) do
      {:ok, ws, data} ->
        case Mint.WebSocket.stream_request_body(state.conn, state.ref, data) do
          {:ok, conn} -> {:ok, %{state | conn: conn, ws: ws}}
          {:error, conn, reason} -> {:error, %{state | conn: conn, ws: ws}, sanitize(reason)}
        end

      {:error, ws, reason} ->
        {:error, %{state | ws: ws}, {:encode, sanitize_frame_error(reason)}}
    end
  end

  @doc """
  Decode an inbound text payload through the bounded Surreal JSON
  profile. Notifications are already separated by `feed/2`; this is for
  RPC replies.
  """
  @spec decode_text(binary()) :: {:ok, Json.json()} | {:error, DrinkMeAlice.Error.t()}
  def decode_text(text), do: Json.decode(text)

  # -- close -----------------------------------------------------------------

  @doc """
  Graceful close: send a close frame at most once, await the peer's close
  under a finite deadline, then close the transport on every path.
  Always releases the owned socket. Returns `:ok`.
  """
  @spec close(t(), timeout()) :: :ok
  def close(%__MODULE__{} = state, timeout \\ 1_000) do
    state =
      if state.close_attempted or is_nil(state.conn) do
        state
      else
        case send_frame(state, {:close, 1_000, ""}) do
          {:ok, state} -> %{state | close_attempted: true}
          {:error, state, _r} -> state
        end
      end

    state = await_peer_close(state, System.monotonic_time(:millisecond) + timeout)
    _ = safe_close_transport(state.conn)
    :ok
  end

  defp await_peer_close(%__MODULE__{conn: nil} = state, _deadline), do: state

  defp await_peer_close(state, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining > 0 do
      case recv_frames(state, remaining) do
        {:ok, state, frames} ->
          if Enum.any?(frames, &match?({:close, _, _}, &1)),
            do: state,
            else: await_peer_close(state, deadline)

        {:timeout, state} ->
          state

        {:closed, state, _} ->
          state

        {:error, state, _} ->
          state
      end
    else
      state
    end
  end

  defp shutdown(state, reason) do
    _ = safe_close_transport(state.conn)
    {:error, reason}
  end

  defp safe_close_transport(nil), do: :ok
  defp safe_close_transport(conn), do: Mint.HTTP.close(conn)

  @doc "Underlying TCP socket (observation only); nil after a fatal error."
  def socket(%__MODULE__{conn: nil}), do: nil
  def socket(%__MODULE__{conn: conn}), do: Mint.HTTP.get_socket(conn)

  # -- error sanitization ------------------------------------------------------

  # Never leak Mint structs, frame payloads, headers or auth material.
  defp sanitize(%Mint.TransportError{reason: r}), do: {:transport, r}
  defp sanitize(%Mint.WebSocketError{reason: r}), do: {:websocket, sanitize_frame_error(r)}
  defp sanitize(%Mint.WebSocket.UpgradeFailureError{status_code: s}), do: {:upgrade_failed, s}
  defp sanitize(r) when is_atom(r), do: r
  defp sanitize(r), do: sanitize_frame_error(r)

  defp sanitize_frame_error({:invalid_utf8, d}) when is_binary(d),
    do: {:invalid_utf8, byte_size(d)}

  defp sanitize_frame_error({:invalid_close_payload, p}) when is_binary(p),
    do: {:invalid_close_payload, byte_size(p)}

  defp sanitize_frame_error({:malformed_payload_length, m}) when is_binary(m),
    do: {:malformed_payload_length, byte_size(m)}

  defp sanitize_frame_error({:malformed_reserved, r}) when is_binary(r),
    do: {:malformed_reserved, byte_size(r)}

  defp sanitize_frame_error({:missing_mask, _}), do: :missing_mask

  defp sanitize_frame_error({:cannot_combine_frames, a, b}) when is_tuple(a) and is_tuple(b),
    do: {:cannot_combine_frames, elem(a, 0), elem(b, 0)}

  defp sanitize_frame_error({:unsupported_opcode, op}) when is_binary(op) and byte_size(op) <= 1,
    do: {:unsupported_opcode, op}

  defp sanitize_frame_error(:insular_continuation), do: :insular_continuation
  defp sanitize_frame_error(:unexpected_continuation), do: :unexpected_continuation
  defp sanitize_frame_error(r) when is_atom(r), do: r
  defp sanitize_frame_error(r) when is_binary(r), do: {:opaque, byte_size(r)}
  defp sanitize_frame_error(r) when is_tuple(r), do: {:opaque_tuple, tuple_size(r)}
  defp sanitize_frame_error(_), do: :unclassified
end
