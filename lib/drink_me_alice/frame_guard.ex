defmodule DrinkMeAlice.FrameGuard do
  @moduledoc """
  Bounded WebSocket framing guard — production adaptation of the accepted
  BF-02A `SurrealTransport.FrameGuard`.

  Mint.WebSocket 1.0.6 has no bound on `buffer` (raw bytes of an
  incomplete frame) or `fragment` (reassembled message payload). This pure
  state machine walks raw socket bytes *before* they reach
  `Mint.WebSocket.decode/2`:

  * parses each frame header itself (14-byte maximum, held in
    `header_acc`) and rejects a declared payload length above
    `:max_message` before Mint can start buffering the body;
  * accumulates the payload total of a fragmented message (data frame +
    continuations) and rejects when the aggregate exceeds `:max_message`
    — the 1 MiB contract message bound, enforced before unbounded decode;
  * enforces the RFC 6455 control-frame rules Mint leaves unchecked
    (payload <= 125 bytes, never fragmented);
  * counts *payload* bytes only — a message of exactly `:max_message`
    payload bytes is accepted; frame headers never consume the allowance.

  The guard retains at most a partial 14-byte header between `feed/2`
  calls; everything else is streamed through position arithmetic, not
  copies.
  """

  @max_header_bytes 14
  @control_opcodes [0x8, 0x9, 0xA]

  defstruct max_message: 1_048_576,
            header_acc: <<>>,
            skip: 0,
            msg_bytes: 0,
            msg_open: false

  @type t :: %__MODULE__{}

  @spec new(keyword()) :: t()
  def new(opts \\ []), do: %__MODULE__{max_message: Keyword.get(opts, :max_message, 1_048_576)}

  @doc """
  Validate a chunk of raw inbound bytes. Returns `{:ok, guard}` or
  `{:error, reason, guard}`; on error the caller must close the transport.
  """
  @spec feed(t(), binary()) :: {:ok, t()} | {:error, term(), t()}
  def feed(%__MODULE__{} = g, bytes) when is_binary(bytes) do
    walk(g, g.header_acc <> bytes)
  end

  defp walk(g, rest) do
    cond do
      byte_size(rest) == 0 ->
        {:ok, %{g | header_acc: <<>>}}

      g.skip > 0 ->
        take = min(g.skip, byte_size(rest))
        walk(%{g | skip: g.skip - take}, binary_part(rest, take, byte_size(rest) - take))

      true ->
        case parse_header(rest) do
          :incomplete ->
            if byte_size(rest) > @max_header_bytes,
              do: {:error, :header_overrun, g},
              else: {:ok, %{g | header_acc: rest}}

          {:ok, opcode, fin?, declared, header_len} ->
            case check_frame(g, opcode, fin?, declared) do
              {:error, reason} ->
                {:error, reason, %{g | header_acc: <<>>}}

              {:ok, g} ->
                g = %{g | header_acc: <<>>, skip: declared}
                walk(g, binary_part(rest, header_len, byte_size(rest) - header_len))
            end
        end
    end
  end

  # Returns :incomplete | {:ok, opcode, fin?, declared, header_len}
  defp parse_header(<<fin::1, _rsv::3, opcode::4, masked::1, len::7, rest::bitstring>>) do
    with {:ok, declared, rest} <- ext_len(len, rest),
         {:ok, mask_len} <- mask_len(masked),
         :ok <- have(rest, mask_len) do
      header_len = 2 + ext_len_size(len) + mask_len
      {:ok, opcode, fin == 1, declared, header_len}
    else
      :incomplete -> :incomplete
    end
  end

  defp parse_header(_), do: :incomplete

  defp ext_len(len, rest) when len in 0..125, do: {:ok, len, rest}

  defp ext_len(126, <<l::unsigned-16, rest::bitstring>>), do: {:ok, l, rest}
  defp ext_len(126, _), do: :incomplete

  defp ext_len(127, <<l::unsigned-64, rest::bitstring>>), do: {:ok, l, rest}
  defp ext_len(127, _), do: :incomplete

  defp ext_len_size(len) when len in 0..125, do: 0
  defp ext_len_size(126), do: 2
  defp ext_len_size(127), do: 8

  defp mask_len(1), do: {:ok, 4}
  defp mask_len(0), do: {:ok, 0}

  defp have(rest, n) when byte_size(rest) >= n, do: :ok
  defp have(_, _), do: :incomplete

  # Per-frame accounting. Any declared payload above max_message is
  # refused before Mint can buffer it; control frames follow RFC 6455
  # limits.
  defp check_frame(g, opcode, fin?, declared) do
    cond do
      opcode in @control_opcodes and declared > 125 ->
        {:error, {:oversized_control, declared}}

      opcode in @control_opcodes and not fin? ->
        {:error, :fragmented_control}

      declared > g.max_message ->
        {:error, {:oversized_declared, declared, g.max_message}}

      opcode == 0x0 ->
        if g.msg_open do
          total = g.msg_bytes + declared

          if total > g.max_message,
            do: {:error, {:oversized_message, total, g.max_message}},
            else: {:ok, %{g | msg_bytes: total, msg_open: not fin?}}
        else
          {:ok, g}
        end

      opcode in [0x1, 0x2] ->
        if g.msg_open do
          {:ok, %{g | msg_open: false, msg_bytes: 0}}
        else
          {:ok, %{g | msg_bytes: declared, msg_open: not fin?}}
        end

      true ->
        {:ok, g}
    end
  end

  @doc "Payload bytes accumulated for the open fragmented message."
  def message_bytes(%__MODULE__{msg_bytes: n}), do: n
end
