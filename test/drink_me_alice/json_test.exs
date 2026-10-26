defmodule DrinkMeAlice.JsonTest do
  @moduledoc """
  Bounded Surreal-dialect JSON decode boundary.

  Regression: root's early review (memory-json-02) showed a 41-digit
  positive integer token was accepted — the byte bound counted the sign.
  Digit count now excludes the optional `-`; positive and negative
  41-digit tokens reject identically.
  """

  use ExUnit.Case, async: false

  alias DrinkMeAlice.Error
  alias DrinkMeAlice.Json

  defp ok(input), do: assert({:ok, _} = Json.decode(input))
  defp err(input), do: assert({:error, %Error{}} = Json.decode(input))

  test "ordinary Surreal reply shapes decode" do
    ok(~s|{"id":7,"result":[{"status":"OK","result":{"op_id":"x"}}]}|)
    ok(~s|[{"a":[1,2,3],"b":{"c":null},"d":true,"e":"s"}]|)
    ok(~s|"just a string"|)
    ok(~s|42|)
  end

  test "empty, non-binary, oversized and trailing input reject" do
    err(<<>>)
    err(<<1, 2, 3>>)
    err(:binary.copy(" ", Json.max_bytes() + 1))
    err(~s|{} {}|)
    err(~s|[1] x|)
  end

  test "depth bound rejects nesting past 16 preflight AND at decode" do
    deep_ok = String.duplicate("[", 16) <> String.duplicate("]", 16)
    deep_bad = String.duplicate("[", 17) <> String.duplicate("]", 17)
    ok(deep_ok)
    err(deep_bad)
    # Brackets inside string literals must not count as depth.
    ok(~s|{"a":"[[[[[[[[[[[[[[[[[[["}|)
  end

  test "object keys bound and duplicate keys" do
    k128 = "{" <> (1..128 |> Enum.map(&~s|"k#{&1}":0|) |> Enum.join(",")) <> "}"
    k129 = "{" <> (1..129 |> Enum.map(&~s|"k#{&1}":0|) |> Enum.join(",")) <> "}"
    ok(k128)
    err(k129)
    err(~s|{"a":1,"a":2}|)
  end

  test "element bound rejects aggregates past 65_536" do
    big = "[" <> (1..65_537 |> Enum.map(&Integer.to_string/1) |> Enum.join(",")) <> "]"
    err(big)
    ok("[" <> (1..65_536 |> Enum.map(&Integer.to_string/1) |> Enum.join(",")) <> "]")
  end

  test "integer digit bound counts digits, not sign — regression" do
    d40 = String.duplicate("9", 40)
    d41 = String.duplicate("9", 41)
    ok(d40)
    ok("-" <> d40)
    # The exact review failure: 41 positive digits were accepted.
    err(d41)
    err("-" <> d41)
    err("1844674407370955161512345678901234567890123456789")
  end

  test "u64-range integers decode exactly" do
    assert {:ok, 18_446_744_073_709_551_615} =
             Json.decode("18446744073709551615")
  end

  test "floats decode; overlong float tokens reject" do
    assert {:ok, 100.0} = Json.decode("1e2")
    assert {:ok, 0.01} = Json.decode("1E-2")
    err("1" <> String.duplicate("0", 70) <> ".0")
  end

  test "string bound enforced pre-allocation by lexical preflight" do
    s_ok = ~s|"| <> String.duplicate("a", 65_536) <> ~s|"|
    s_bad = ~s|"| <> String.duplicate("a", 65_537) <> ~s|"|
    ok(s_ok)
    assert {:error, %Error{code: "bounded_limit_exceeded"}} = Json.decode(s_bad)

    # Escapes only shrink the decoded value: a raw literal at the bound
    # made of \" pairs decodes to half its span and must pass.
    esc = ~s|"| <> String.duplicate(~s|\\"|, 32_768) <> ~s|"|
    assert {:ok, decoded} = Json.decode(esc)
    assert byte_size(decoded) == 32_768
  end

  test "review-06: decoded-size bound, not raw-span — escaped content round-trips" do
    # A 40,000-byte decoded string of quotes: raw span 80,012 > the
    # bound, decoded 40,000 <= it — the bound measures DECODED bytes.
    quotes_decoded = String.duplicate("\"", 40_000)
    {:ok, quotes_wire} = Json.encode(%{"s" => quotes_decoded})
    assert byte_size(quotes_wire) > 65_536
    assert {:ok, %{"s" => ^quotes_decoded}} = Json.decode(quotes_wire)

    # Same for a 40,000-byte decoded string of backslashes (raw span
    # doubles through `\\` escapes).
    slashes_decoded = String.duplicate("\\", 40_000)
    {:ok, slashes_wire} = Json.encode(%{"s" => slashes_decoded})
    assert byte_size(slashes_wire) > 65_536
    assert {:ok, %{"s" => ^slashes_decoded}} = Json.decode(slashes_wire)
  end

  test "review-06: unterminated oversized literals still hit the bound" do
    # Unterminated 65,537-byte content — malformed, but the decoded-size
    # bound must still fire before the bytes reach OTP.
    unterminated = ~s|"| <> String.duplicate("a", 65_537)
    assert {:error, %Error{code: "bounded_limit_exceeded"}} = Json.preflight(unterminated)
    assert {:error, %Error{}} = Json.decode(unterminated)

    # Unterminated with escapes: 65,537 decoded bytes of \n content.
    unterminated_esc = ~s|"| <> String.duplicate(~s|\\n|, 65_537)
    assert {:error, %Error{code: "bounded_limit_exceeded"}} = Json.preflight(unterminated_esc)
    assert {:error, %Error{}} = Json.decode(unterminated_esc)

    # An unterminated literal UNDER the bound stays a decode-layer
    # malformed error — preflight is a bound check, not a validator.
    under = ~s|"| <> String.duplicate("a", 100)
    assert :ok = Json.preflight(under)
    assert {:error, %Error{code: "malformed_message"}} = Json.decode(under)
  end

  test "review-06: surrogate-pair escapes count as one 4-byte scalar" do
    # 😀 = 😀 — one decoded scalar, 4 bytes. A literal of
    # 16,384 pairs = 65,536 decoded bytes must pass.
    pairs = String.duplicate("\\uD83D\\uDE00", 16_384)
    wire = ~s|"| <> pairs <> ~s|"|
    assert :ok = Json.preflight(wire)
    assert {:ok, decoded} = Json.decode(wire)
    assert byte_size(decoded) == 65_536

    # One pair past the bound rejects.
    over = ~s|"| <> pairs <> "\\uD83D\\uDE00" <> ~s|"|
    assert {:error, %Error{code: "bounded_limit_exceeded"}} = Json.preflight(over)
  end

  test "invalid utf-8 inside strings rejects" do
    # Literal backslash-u surrogate escape in the JSON text — the decoder
    # must reject the lone surrogate, not the test's string literal.
    err(~s|{"a":"| <> "\\uD800" <> ~s|"}|)
  end

  test "preflight alone enforces the literal bound before decode allocates" do
    # A literal past the bound inside an otherwise-valid envelope is
    # refused by preflight even though decode callbacks would also catch it.
    oversized = ~s|{"s":"| <> String.duplicate("z", 70_000) <> ~s|"}|
    assert {:error, %Error{code: "bounded_limit_exceeded"}} = Json.preflight(oversized)
    assert :ok = Json.preflight(~s|{"s":"ok"}|)
  end

  test "decode amplification stays proportional to bounded input" do
    # Worst amplification shape under the bounds: maximal element count
    # of small scalars. Measure term words + decoding process memory.
    input = "[" <> (1..65_536 |> Enum.map(&Integer.to_string/1) |> Enum.join(",")) <> "]"
    assert byte_size(input) <= Json.max_bytes()

    parent = self()

    {_pid, ref} =
      spawn_monitor(fn ->
        before = :erlang.memory(:total)
        {:ok, term} = Json.decode(input)
        during = :erlang.memory(:total)
        send(parent, {:measured, Json.term_words(term), during - before})
      end)

    receive do
      {:measured, words, vm_delta} ->
        wait_down(ref)
        term_bytes = words * 8
        # Honest label: term machine-word footprint vs input bytes.
        # ~4 MiB worst term for a 1 MiB worst input on this shape.
        assert term_bytes <= 8 * byte_size(input),
               "amplification #{term_bytes}/#{byte_size(input)}"

        IO.inspect(%{
          input_bytes: byte_size(input),
          term_words: words,
          term_bytes: term_bytes,
          vm_total_delta_bytes: vm_delta
        })
    after
      10_000 -> raise "decode did not return"
    end
  end

  defp wait_down(ref) do
    receive do
      {:DOWN, ^ref, :process, _, _} -> :ok
    after
      2_000 -> raise "decoder process not reaped"
    end
  end

  test "encode: nil is JSON null (OTP spells nil as \"nil\"), and round-trips" do
    msg = %{"id" => 1, "params" => [nil, %{"a" => nil, "b" => [true, false, 1, "x"]}]}
    assert {:ok, raw} = Json.encode(msg)
    refute raw =~ ~s("nil")
    assert {:ok, ^msg} = Json.decode(raw)
  end

  test "encode: only canonical terms, within the decode bounds" do
    assert {:error, %Error{code: "invalid_input", detail: :noncanonical_term}} =
             Json.encode(%{"f" => 1.5})

    assert {:error, %Error{code: "invalid_input", detail: :noncanonical_term}} =
             Json.encode(%{"a" => :atom})

    assert {:error, %Error{code: "invalid_input", detail: :non_string_key}} = Json.encode(%{a: 1})

    assert {:error, %Error{code: "malformed_message", detail: :integer_range}} =
             Json.encode(2 ** 63)

    assert {:error, %Error{code: "malformed_message", detail: :invalid_utf8}} =
             Json.encode(<<0xFF>>)

    deep = Enum.reduce(1..17, 1, fn _, acc -> [acc] end)

    assert {:error, %Error{code: "bounded_limit_exceeded", detail: :nesting_depth}} =
             Json.encode(deep)
  end
end
