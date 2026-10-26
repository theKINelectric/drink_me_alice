defmodule DrinkMeAlice.SessionTest do
  @moduledoc """
  Session setup regressions (A-01): an omitted optional scope key and an
  explicit nil are the same configuration; a failed setup returns a
  typed result — never an exception carrying the credential map — and
  never leaks the acquired socket.

  `FakeSurreal` accepts exactly one WebSocket connection per instance,
  so each probe owns its own peer. Port accounting is whole-cycle: the
  baseline is taken before any peer starts and re-asserted after every
  peer has stopped, so the peers' own listener/accepted sockets never
  pollute the count — a session socket the library failed to release
  survives peer teardown and shows up.
  """

  use ExUnit.Case, async: false

  alias DrinkMeAlice.{FakeSurreal, Session}

  @secret "session-secret-" <> Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)

  defp port_count, do: length(Port.list())

  defp assert_no_leaked_ports(base) do
    deadline = System.monotonic_time(:millisecond) + 2_000
    wait_ports(base, deadline)
  end

  defp wait_ports(base, deadline) do
    if port_count() == base do
      :ok
    else
      unless System.monotonic_time(:millisecond) >= deadline do
        Process.sleep(50)
        wait_ports(base, deadline)
      else
        flunk("socket leak: #{port_count()} open ports, baseline #{base}")
      end
    end
  end

  defp root_config(fake, extra \\ []) do
    Map.merge(
      %{host: "127.0.0.1", port: fake.port, user: "root", pass: @secret},
      Map.new(extra)
    )
  end

  setup_all do
    # The first socket cycle in a VM lazily creates one persistent port
    # (measured: cycle 1 delta +1, cycles 2..n delta 0). Absorb it here
    # so per-test port deltas can only mean leaks.
    {:ok, fake} = FakeSurreal.start()
    {:ok, s} = Session.open(%{host: "127.0.0.1", port: fake.port, user: "warm", pass: "up"})
    Session.close(s)
    FakeSurreal.stop(fake)
    :ok
  end

  test "root config with omitted ns/db opens like explicit nils (A-01)" do
    base = port_count()
    {:ok, fake_a} = FakeSurreal.start()
    {:ok, fake_b} = FakeSurreal.start()

    try do
      # The audited reproduction: dropping the keys raised a KeyError
      # whose message printed this map — including `pass` — after the
      # socket was already connected, and the socket stayed open.
      omitted = Session.open(root_config(fake_a))
      explicit = Session.open(root_config(fake_b, ns: nil, db: nil))

      assert {:ok, s_a} = omitted
      assert {:ok, s_b} = explicit
      refute inspect(omitted) =~ @secret
      refute inspect(explicit) =~ @secret

      assert {:ok, %Session{}, _} = Session.query(s_a, "RETURN 1;")
      assert {:ok, %Session{}, _} = Session.query(s_b, "RETURN 1;")

      Session.close(s_a)
      Session.close(s_b)
    after
      FakeSurreal.stop(fake_a)
      FakeSurreal.stop(fake_b)
    end

    assert_no_leaked_ports(base)
  end

  test "namespace scope with omitted db behaves like db: nil (A-01)" do
    base = port_count()
    {:ok, fake_a} = FakeSurreal.start()
    {:ok, fake_b} = FakeSurreal.start()

    try do
      assert {:ok, s_a} = Session.open(root_config(fake_a, ns: "nsx", signin: :namespace))

      assert {:ok, s_b} =
               Session.open(root_config(fake_b, ns: "nsx", db: nil, signin: :namespace))

      Session.close(s_a)
      Session.close(s_b)
    after
      FakeSurreal.stop(fake_a)
      FakeSurreal.stop(fake_b)
    end

    assert_no_leaked_ports(base)
  end

  test "signin refusal returns a typed error, closes the socket, keeps the secret out (A-01)" do
    base = port_count()
    {:ok, fake} = FakeSurreal.start()

    :ok =
      FakeSurreal.command(
        fake,
        {:set_handler,
         fn
           "signin", _params -> {:error_reply, -32_002, "invalid credentials"}
           method, params -> FakeSurreal.default(method, params)
         end}
      )

    try do
      result = Session.open(root_config(fake))

      assert {:error, {:rpc_error, -32_002}} = result
      refute inspect(result) =~ @secret
      refute inspect(result) =~ "pass"
    after
      FakeSurreal.stop(fake)
    end

    assert_no_leaked_ports(base)
  end

  test "no malformed configuration raises or leaks a socket (A-01 belt)" do
    base = port_count()
    {:ok, fake} = FakeSurreal.start()

    configs = [
      root_config(fake) |> Map.drop([:ns, :db]),
      root_config(fake, ns: nil, db: nil),
      root_config(fake) |> Map.drop([:host]),
      root_config(fake) |> Map.drop([:port]),
      root_config(fake) |> Map.drop([:user]),
      root_config(fake) |> Map.drop([:pass]),
      root_config(fake, ns: "bad ns"),
      root_config(fake, db: "has space"),
      root_config(fake, ns: nil, db: "orphan"),
      root_config(fake, signin: "nope"),
      root_config(fake, reply_budget_ms: :soon),
      root_config(fake, reply_budget_ms: 61_000),
      root_config(fake, port: "8000"),
      root_config(fake, host: nil),
      # Review R-06: an out-of-range INTEGER port passed the old
      # `is_integer` check and exited inside the TCP layer.
      root_config(fake, port: -1),
      root_config(fake, port: 0),
      root_config(fake, port: 65_536),
      # Invalid timeout options would reach the socket options the
      # same way.
      root_config(fake, connect_timeout: :soon),
      root_config(fake, connect_timeout: -5),
      # Malformed top-level forms are typed errors too (R-06), never
      # an ArgumentError from Map.new/1.
      [:not, :a, :keyword, :list],
      "just a string",
      {"tuple", "config"},
      :atom
    ]

    try do
      for c <- configs do
        # The contract under test: EVERY configuration shape yields a
        # typed result. An exception here would print the map (and the
        # generated password) into ordinary error reporting.
        result = Session.open(c)
        assert elem(result, 0) in [:ok, :error]

        case result do
          {:ok, s} -> Session.close(s)
          _ -> :ok
        end

        refute inspect(result) =~ @secret
      end
    after
      FakeSurreal.stop(fake)
    end

    assert_no_leaked_ports(base)
  end
end
