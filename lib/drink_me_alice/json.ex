defmodule DrinkMeAlice.Json do
  @moduledoc """
  Bounded decoder/encoder boundary for SurrealDB JSON-RPC payloads.

  Surreal replies are
  ordinary JSON (floats may appear, integer tokens may exceed i64, object
  key order is unspecified), so this decoder runs OTP `:json.decode/3`
  behind explicit structural and decoded-term limits:

  - input bytes bounded by the caller (the WebSocket frame guard already
    caps a message at `max_bytes`, default #{1_048_576}); checked again
    here so the bound does not depend on transport state;
  - container depth <= #{16} (RPC envelopes plus one level of record
    content stay far below this);
  - object keys <= #{128} per object, duplicate *decoded* keys rejected;
  - aggregate document elements <= #{65_536};
  - each decoded string <= #{65_536} bytes and valid UTF-8;
  - integer tokens <= #{40} digits *excluding the optional sign*
    (Erlang integers are exact at any width; the bound stops digit-string
    amplification);
  - float tokens <= #{64} bytes, decoded with `Float` semantics; the dialect may
    legally carry floats, so they are accepted and *counted* rather than
    rejected by shape.

  Two enforcement layers, honestly labeled:

  1. `preflight/1` — a single linear, allocation-free scan over the raw
     bytes *before* `:json.decode` runs. It counts every string
     literal's DECODED UTF-8 size (escape-aware: `\\uXXXX` pairs count
     at their recombined width, so a 40 KiB value of `\\"` escapes —
     whose raw span doubles its decoded size — passes while an oversized
     decoded value is rejected) and container nesting, rejecting
     violations before OTP has allocated anything for them. This is the
     pre-allocation bound — the string *callback* below is only a
     post-construction backstop, and is documented as such.
  2. Decode callbacks — container start/push and every scalar enforce
     the bounds *while* decoding, aborting before an unbounded term is
     built. `term_words/1` and process-memory measurements in the tests
     supply the decode-amplification evidence the card requires.

  Encoding (`encode/1`) accepts only canonical terms under the same
  structural bounds, checked before OTP `:json.encode/1` runs.
  """

  alias DrinkMeAlice.Error

  @max_bytes 1_048_576
  @max_depth 16
  @max_keys 128
  @max_string_bytes 65_536
  @max_elements 65_536
  @max_integer_digits 40
  @max_float_bytes 64

  @type json :: map() | list() | binary() | integer() | float() | boolean() | nil

  @doc "Configured input byte bound (the 1 MiB message bound)."
  @spec max_bytes() :: pos_integer()
  def max_bytes, do: @max_bytes

  @doc "The declared structural/decoded-term bounds for this profile."
  @spec bounds() :: map()
  def bounds do
    %{
      max_bytes: @max_bytes,
      max_depth: @max_depth,
      max_keys_per_object: @max_keys,
      max_string_bytes: @max_string_bytes,
      max_elements: @max_elements,
      max_integer_digits: @max_integer_digits,
      max_float_bytes: @max_float_bytes
    }
  end

  @doc """
  Decode one Surreal-dialect JSON document under the bounded profile.

  `{:ok, term}` | `{:error, %Error{}}` — `malformed_message` for syntax
  violations, `bounded_limit_exceeded` for bound breaches, both with a
  bounded tag and never an inspected input fragment.
  """
  @spec decode(binary()) :: {:ok, json()} | {:error, Error.t()}
  def decode(input) when is_binary(input) do
    cond do
      input == <<>> ->
        {:error, Error.malformed(:empty_input)}

      byte_size(input) > @max_bytes ->
        {:error, Error.bounded_exceeded(:message_bytes)}

      true ->
        case preflight(input) do
          :ok -> run(input)
          {:error, %Error{} = e} -> {:error, e}
        end
    end
  end

  def decode(_other), do: {:error, Error.malformed(:not_binary_input)}

  @doc """
  Lexical pre-pass over raw input before `:json.decode` allocates.
  Returns `:ok` or `{:error, %Error{}}`. Enforces, allocation-free:

  - every string literal's *decoded* UTF-8 size <= #{65_536} bytes —
    counted while scanning, so a 40 KiB value of `\\"` escapes (whose
    raw span is double its decoded size) passes while an oversized
    decoded value is rejected before OTP allocates it. Escape widths:
    `\\X` → 1 byte; `\\uXXXX` → the code point's UTF-8 width (1–3),
    with a high surrogate held pending so a `\\uD800..\\uDBFF` +
    `\\uDC00..\\uDFFF` pair counts exactly 4. An unterminated string
    keeps counting to end of input — oversized decoded content is
    rejected at the bound even though the malformed literal would fail
    decode anyway;
  - container nesting depth <= #{16}.

  This is a bound check, not a validator — malformed JSON still reaches
  the decoder, which rejects it through the same typed error surface.
  """
  @spec preflight(binary()) :: :ok | {:error, Error.t()}
  def preflight(input) when is_binary(input) do
    scan(input, false, 0, 0)
  end

  # str = string scan state `false | {decoded_bytes, pending_hi}` where
  # pending_hi is `true` after a `\\uD800..\\uDBFF` escape awaiting its
  # low-surrogate partner; depth = container nesting.
  defp scan(<<>>, false, _str, _depth), do: :ok

  # End of input inside a string literal: check the decoded count the
  # same way — the literal is malformed, but the bound must still be
  # enforced before the bytes reach OTP.
  defp scan(<<>>, true, {n, pending}, _depth) do
    n = if pending, do: n + 3, else: n

    if n > @max_string_bytes,
      do: {:error, Error.bounded_exceeded(:string_bytes)},
      else: :ok
  end

  defp scan(<<c, rest::binary>>, in_str, str, depth) do
    cond do
      in_str and c == ?\\ ->
        escaped(rest, str, depth)

      in_str and c == ?" ->
        {n, pending} = str
        n = if pending, do: n + 3, else: n

        if n > @max_string_bytes do
          {:error, Error.bounded_exceeded(:string_bytes)}
        else
          scan(rest, false, 0, depth)
        end

      in_str ->
        # Raw content byte. UTF-8 continuation bytes each count — the
        # decoded size is the raw byte count for unescaped content.
        case bump(str, 1) do
          {:ok, str} -> scan(rest, true, str, depth)
          :over -> {:error, Error.bounded_exceeded(:string_bytes)}
        end

      c == ?" ->
        scan(rest, true, {0, false}, depth)

      c in [?[, ?{] ->
        if depth + 1 > @max_depth do
          {:error, Error.bounded_exceeded(:nesting_depth)}
        else
          scan(rest, false, 0, depth + 1)
        end

      c in [?], ?}] ->
        scan(rest, false, 0, max(depth - 1, 0))

      true ->
        scan(rest, false, 0, depth)
    end
  end

  # `\\` escape inside a string: `\X` decodes to 1 byte; `\uXXXX`
  # decodes to the code point's UTF-8 width. A pending high surrogate
  # followed by `\uDC00..\uDFFF` completes a pair at 4 bytes; followed
  # by anything else the literal is malformed — the unpaired half is
  # still flushed at its 3-byte surrogate width so the bound holds on
  # malformed input too.
  defp escaped(<<>>, str, depth), do: scan(<<>>, true, flush(str), depth)

  defp escaped(<<e, rest::binary>>, str, depth) do
    case e do
      ?u ->
        case rest do
          <<h4::binary-size(4), rest2::binary>> ->
            case hex4(h4) do
              {:cp, cp} when cp in 0xD800..0xDBFF ->
                # High half held pending — contributes 0 until a low
                # partner completes it as one 4-byte scalar.
                {n, _p} = flush(str)
                cont(bump({n, false}, 0) |> put_pending(), rest2, depth)

              {:cp, cp} when cp in 0xDC00..0xDFFF ->
                case str do
                  {n, true} -> cont(bump({n, false}, 4), rest2, depth)
                  _ -> cont(bump(flush(str), 3), rest2, depth)
                end

              {:cp, cp} ->
                cont(bump(flush(str), utf8_len(cp)), rest2, depth)

              :error ->
                # Malformed escape — the decoder rejects it; count it
                # as 1 so scanning continues honestly.
                cont(bump(flush(str), 1), rest2, depth)
            end

          short ->
            # Fewer than 4 bytes left — unterminated/malformed tail.
            # Finish the scan over what remains so the decoded bound
            # (and any pending half) is still enforced at EOF.
            scan(short, true, flush(str), depth)
        end

      _single ->
        cont(bump(flush(str), 1), rest, depth)
    end
  end

  defp put_pending({:ok, {n, _p}}), do: {:ok, {n, true}}
  defp put_pending(:over), do: :over

  defp cont({:ok, str}, rest, depth), do: scan(rest, true, str, depth)
  defp cont(:over, _rest, _depth), do: {:error, Error.bounded_exceeded(:string_bytes)}

  # A pending high surrogate not completed by a low surrogate is
  # malformed but still counted at its 3-byte surrogate width.
  defp flush({n, true}), do: {n + 3, false}
  defp flush(str), do: str

  defp bump({n, p}, add) do
    if n + add > @max_string_bytes, do: :over, else: {:ok, {n + add, p}}
  end

  defp hex4(<<a, b, c, d>>) do
    with {:ok, hi} <- hex2(a, b), {:ok, lo} <- hex2(c, d) do
      {:cp, hi * 256 + lo}
    end
  end

  defp hex2(a, b) do
    with {:ok, x} <- hex1(a), {:ok, y} <- hex1(b) do
      {:ok, x * 16 + y}
    end
  end

  defp hex1(c) when c in ?0..?9, do: {:ok, c - ?0}
  defp hex1(c) when c in ?a..?f, do: {:ok, c - ?a + 10}
  defp hex1(c) when c in ?A..?F, do: {:ok, c - ?A + 10}
  defp hex1(_), do: :error

  defp utf8_len(cp) when cp <= 0x7F, do: 1
  defp utf8_len(cp) when cp <= 0x7FF, do: 2
  defp utf8_len(_cp), do: 3

  defp run(input) do
    acc0 = %{elements: 0, depth: 0}

    decoders = %{
      array_start: &array_start(&1),
      array_push: &array_push(&1, &2),
      array_finish: &array_finish/2,
      object_start: &object_start(&1),
      object_push: &object_push(&1, &2, &3),
      object_finish: &object_finish/2,
      float: &float_token/1,
      integer: &integer_token/1,
      string: &string_value/1,
      null: nil
    }

    try do
      {value, acc, rest} = :json.decode(input, acc0, decoders)

      case drop_ws(rest) do
        <<>> when acc.elements <= @max_elements -> {:ok, value}
        <<>> -> {:error, Error.bounded_exceeded(:document_elements)}
        _ -> {:error, Error.malformed(:trailing_input)}
      end
    rescue
      _e -> {:error, Error.malformed(:json_syntax)}
    catch
      {:drink_me_alice_error, %Error{} = e} -> {:error, e}
    end
  end

  defp drop_ws(<<c, rest::binary>>) when c in [0x20, 0x09, 0x0A, 0x0D], do: drop_ws(rest)
  defp drop_ws(rest), do: rest

  # A container may open while its own depth stays within the bound; its
  # members sit one deeper.
  defp array_start(%{depth: d} = outer) when d < @max_depth do
    %{elements: outer.elements, depth: d + 1, items: []}
  end

  defp array_start(_outer),
    do: throw({:drink_me_alice_error, Error.bounded_exceeded(:nesting_depth)})

  defp array_push(value, acc) do
    if acc.depth <= @max_depth do
      n = acc.elements + 1

      if n > @max_elements,
        do: throw({:drink_me_alice_error, Error.bounded_exceeded(:document_elements)}),
        else: %{acc | elements: n, items: [value | acc.items]}
    else
      throw({:drink_me_alice_error, Error.bounded_exceeded(:nesting_depth)})
    end
  end

  defp array_finish(acc, outer) do
    {Enum.reverse(acc.items), %{outer | elements: acc.elements}}
  end

  defp object_start(%{depth: d} = outer) when d < @max_depth do
    %{elements: outer.elements, depth: d + 1, kc: 0, keys: MapSet.new(), items: []}
  end

  defp object_start(_outer),
    do: throw({:drink_me_alice_error, Error.bounded_exceeded(:nesting_depth)})

  defp object_push(key, value, acc) do
    cond do
      MapSet.member?(acc.keys, key) ->
        throw({:drink_me_alice_error, Error.malformed(:duplicate_key)})

      acc.kc >= @max_keys ->
        throw({:drink_me_alice_error, Error.bounded_exceeded(:object_keys)})

      acc.depth > @max_depth ->
        throw({:drink_me_alice_error, Error.bounded_exceeded(:nesting_depth)})

      acc.elements + 1 > @max_elements ->
        throw({:drink_me_alice_error, Error.bounded_exceeded(:document_elements)})

      true ->
        %{
          acc
          | elements: acc.elements + 1,
            kc: acc.kc + 1,
            keys: MapSet.put(acc.keys, key),
            items: [{key, value} | acc.items]
        }
    end
  end

  defp object_finish(acc, outer) do
    {Map.new(Enum.reverse(acc.items)), %{outer | elements: acc.elements}}
  end

  # The bound counts *digits*, with the optional leading `-` excluded —
  # a 41-digit positive token must reject exactly like a 41-digit
  # negative one.
  defp integer_token(token) do
    digits =
      case token do
        <<"-", rest::binary>> -> byte_size(rest)
        _ -> byte_size(token)
      end

    if digits <= @max_integer_digits do
      try do
        :erlang.binary_to_integer(token)
      rescue
        _ -> throw({:drink_me_alice_error, Error.malformed(:integer_token)})
      end
    else
      throw({:drink_me_alice_error, Error.malformed(:integer_range)})
    end
  end

  defp float_token(token) when byte_size(token) <= @max_float_bytes do
    try do
      :erlang.binary_to_float(token)
    rescue
      _ -> throw({:drink_me_alice_error, Error.malformed(:float_token)})
    end
  end

  defp float_token(_token), do: throw({:drink_me_alice_error, Error.malformed(:float_token)})

  defp string_value(s) when is_binary(s) do
    cond do
      byte_size(s) > @max_string_bytes ->
        throw({:drink_me_alice_error, Error.bounded_exceeded(:string_bytes)})

      not String.valid?(s) ->
        throw({:drink_me_alice_error, Error.malformed(:invalid_utf8)})

      true ->
        s
    end
  end

  @doc """
  Encode an outbound message. Only canonical terms travel: string-keyed
  maps, lists, valid UTF-8 binaries, i64 integers, booleans and `nil` —
  no floats, atoms or structs (send those as strings with a SurrealQL
  cast). The decode bounds apply (depth, keys, string bytes, elements),
  checked before OTP `:json.encode/1` runs; output is at most
  #{@max_bytes} bytes.
  """
  @spec encode(term()) :: {:ok, binary()} | {:error, Error.t()}
  def encode(value) do
    _ = canonical(value, 1, 0)
    raw = value |> :json.encode(&encode_value/2) |> IO.iodata_to_binary()

    if byte_size(raw) <= @max_bytes,
      do: {:ok, raw},
      else: {:error, Error.bounded_exceeded(:message_bytes)}
  catch
    {:drink_me_alice_error, %Error{} = e} -> {:error, e}
  end

  # OTP's encoder spells Elixir `nil` as the string "nil" (only the atom
  # `null` is JSON null) — map it explicitly.
  defp encode_value(nil, _encode), do: "null"
  defp encode_value(value, encode), do: :json.encode_value(value, encode)

  @i64_min -9_223_372_036_854_775_808
  @i64_max 9_223_372_036_854_775_807

  # Walks the term once, returning the running element count; throws a
  # typed error at the first violation.
  defp canonical(_v, depth, _n) when depth > @max_depth,
    do: reject(Error.bounded_exceeded(:nesting_depth))

  defp canonical(v, _d, n) when is_nil(v) or is_boolean(v), do: n
  defp canonical(v, _d, n) when is_integer(v) and v >= @i64_min and v <= @i64_max, do: n
  defp canonical(v, _d, _n) when is_integer(v), do: reject(Error.malformed(:integer_range))

  defp canonical(v, _d, n) when is_binary(v) do
    cond do
      byte_size(v) > @max_string_bytes -> reject(Error.bounded_exceeded(:string_bytes))
      not String.valid?(v) -> reject(Error.malformed(:invalid_utf8))
      true -> n
    end
  end

  defp canonical(v, d, n) when is_list(v),
    do: Enum.reduce(v, n, &canonical(&1, d + 1, element(&2)))

  defp canonical(v, d, n) when is_map(v) and not is_struct(v) do
    if map_size(v) > @max_keys, do: reject(Error.bounded_exceeded(:object_keys))

    Enum.reduce(v, n, fn
      {k, x}, n when is_binary(k) -> canonical(x, d + 1, element(canonical(k, d, n)))
      _pair, _n -> reject(Error.invalid(:non_string_key))
    end)
  end

  defp canonical(_v, _d, _n), do: reject(Error.invalid(:noncanonical_term))

  defp element(n) when n >= @max_elements, do: reject(Error.bounded_exceeded(:document_elements))
  defp element(n), do: n + 1

  defp reject(%Error{} = e), do: throw({:drink_me_alice_error, e})

  @doc """
  Heap size of a decoded term in machine words
  (`:erts_debug.flat_size`, 8 bytes/word on this target). Honest scope:
  this counts *heap* words only — off-heap binary bytes (>64B
  refc binaries) are excluded by `flat_size`. RSS/binary-allocation
  evidence is a separate Q01 measurement; this helper must not be cited
  as covering it.
  """
  @spec term_words(term()) :: non_neg_integer()
  def term_words(term), do: :erts_debug.flat_size(term)
end
