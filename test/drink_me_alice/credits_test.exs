defmodule DrinkMeAlice.CreditsTest do
  @moduledoc """
  Regression coverage for the slot-table credit design.

  Root's early reviews reproduced two races in the counter+holder design
  (leaked credit on holder death `{1,26}`, double-release `{-1,26}`) and
  flagged the send/mark gap that could orphan a queued payload. The
  payload-in-slot design removes all three: payloads live in the slot
  row, the mailbox carries only fixed-size doorbells, and every
  transition is one atomic `select_replace` on `{n, epoch}`.
  """

  use ExUnit.Case, async: false

  alias DrinkMeAlice.Credits

  # Payload shape: {reply_alias, request, deadline_mono_ms} — any term;
  # tests use a ref as the reply destination.
  defp payload(request \\ %{"m" => "x"}) do
    {make_ref(), request, System.monotonic_time(:millisecond) + 2_000}
  end

  defp wait_down(ref, timeout \\ 2_000) do
    receive do
      {:DOWN, ^ref, :process, _, _} -> :ok
    after
      timeout -> raise "child not reaped"
    end
  end

  defp eventually(fun, deadline \\ 2_000) do
    if fun.() do
      true
    else
      if deadline <= 0 do
        false
      else
        Process.sleep(10)
        eventually(fun, deadline - 10)
      end
    end
  end

  test "reserve marks exactly one slot with a unique epoch token" do
    tab = Credits.new()
    {:ok, {n, epoch}} = Credits.reserve(tab, :general)

    assert is_integer(epoch) and epoch > 0
    assert [{^n, :reserved, :general, pid, ^epoch, 0, nil}] = :ets.lookup(tab, n)
    assert pid == self()
    assert Credits.snapshot(tab).general == {1, 26}
  end

  test "enqueue installs the payload atomically; take returns it; finish frees" do
    tab = Credits.new()
    {:ok, {n, epoch} = token} = Credits.reserve(tab, :general)
    pl = payload()

    assert :ok = Credits.enqueue(tab, token, pl)
    assert [{^n, :enqueued, :general, _pid, ^epoch, _ms, ^pl}] = :ets.lookup(tab, n)
    assert Credits.snapshot(tab).enqueued == 1

    # Owner claims the slot — the payload comes back intact.
    assert {:ok, ^pl} = Credits.take(tab, n, epoch)
    assert [{^n, :processing, :general, _owner, ^epoch, _ms, ^pl}] = :ets.lookup(tab, n)
    assert Credits.take(tab, n, epoch) == :none

    assert :ok = Credits.finish(tab, token)
    assert [{^n, :free, :general, 0, 0, 0, nil}] = :ets.lookup(tab, n)
    assert Credits.snapshot(tab).general == {0, 26}
  end

  test "take on a merely-reserved slot claims nothing" do
    tab = Credits.new()
    {:ok, {n, epoch}} = Credits.reserve(tab, :general)
    assert :none = Credits.take(tab, n, epoch)
    assert Credits.snapshot(tab).general == {1, 26}
  end

  test "enqueued/1 lists enqueued payloads oldest-first" do
    tab = Credits.new()

    {t1, t2} = {elem(Credits.reserve(tab, :general), 1), elem(Credits.reserve(tab, :general), 1)}
    :ok = Credits.enqueue(tab, t1, payload(%{"m" => "a"}))
    Process.sleep(2)
    :ok = Credits.enqueue(tab, t2, payload(%{"m" => "b"}))

    assert [{n1, e1, ms1, _}, {n2, e2, ms2, _}] = Credits.enqueued(tab)
    assert {n1, e1} == t1 and {n2, e2} == t2 and ms1 <= ms2
  end

  test "exhaustion is typed, never queued" do
    tab = Credits.new()
    tokens = for _ <- 1..26, do: elem(Credits.reserve(tab, :general), 1)
    assert {:error, :exhausted} = Credits.reserve(tab, :general)
    assert Credits.snapshot(tab).general == {26, 26}
    for t <- tokens, do: Credits.release(tab, t)
    assert Credits.snapshot(tab).general == {0, 26}
  end

  test "internal class has its own six slots; 32 total is the whole bound" do
    tab = Credits.new()
    for _ <- 1..6, do: {:ok, _} = Credits.reserve(tab, :internal)
    assert {:error, :exhausted} = Credits.reserve(tab, :internal)
    assert {:ok, _} = Credits.reserve(tab, :general)
    s = Credits.snapshot(tab)
    assert s.internal == {6, 6}
    assert s.general == {1, 26}
  end

  test "dead holder in :reserved is reclaimed by sweep — no payload to orphan" do
    tab = Credits.new()
    test_pid = self()

    {pid, ref} =
      spawn_monitor(fn ->
        {:ok, _token} = Credits.reserve(tab, :general)
        send(test_pid, :reserved)

        receive do
          _ -> :ok
        after
          5_000 -> :ok
        end
      end)

    assert_receive :reserved, 2_000

    assert eventually(fn ->
             Enum.any?(:ets.tab2list(tab), fn
               {_n, :reserved, :general, found, _, _, nil} when found == pid -> true
               _ -> false
             end)
           end)

    Process.exit(pid, :kill)
    wait_down(ref)

    assert Credits.sweep(tab) >= 1
    assert Credits.snapshot(tab).general == {0, 26}
  end

  test "dead holder in :enqueued is never swept — the owner still owns the payload" do
    tab = Credits.new()
    test_pid = self()

    {_pid, ref} =
      spawn_monitor(fn ->
        {:ok, token} = Credits.reserve(tab, :general)
        # Payload installed with the *test* process as reply target —
        # the slot belongs to the owner once enqueued, holder liveness
        # is irrelevant to the payload.
        :ok = Credits.enqueue(tab, token, {make_ref(), %{"m" => "x"}, 0})
        send(test_pid, :enqueued)
      end)

    assert_receive :enqueued, 2_000
    wait_down(ref)

    # Sweep must not reclaim it: an enqueued row always carries a real
    # payload whose accounting is the owner's, not the dead holder's.
    assert Credits.sweep(tab) == 0
    assert Credits.snapshot(tab).general == {1, 26}

    # The owner drains it anyway — deadline 0 means eligible-now.
    [{n, epoch, _ms, {_alias, _req, 0}}] = Credits.enqueued(tab)
    assert {:ok, {_alias, %{"m" => "x"}, 0}} = Credits.take(tab, n, epoch)
    assert :ok = Credits.finish(tab, {n, epoch})
    assert Credits.snapshot(tab).general == {0, 26}
  end

  test "release is a no-op on an enqueued slot — caller cannot free a live payload" do
    tab = Credits.new()
    {:ok, {n, epoch} = token} = Credits.reserve(tab, :general)
    :ok = Credits.enqueue(tab, token, payload())

    assert :ok = Credits.release(tab, token)
    assert [{^n, :enqueued, :general, _, ^epoch, _, _}] = :ets.lookup(tab, n)
    assert Credits.snapshot(tab).general == {1, 26}
  end

  test "concurrent and repeated releases free at most once" do
    tab = Credits.new()
    {:ok, token} = Credits.reserve(tab, :general)

    # Foreign-pid releases race on the same token — the pid-pinned CAS
    # matches none of them.
    for _ <- 1..4, do: spawn(fn -> Credits.release(tab, token) end)
    assert eventually(fn -> Credits.snapshot(tab).general == {1, 26} end)

    # The holder's own release frees exactly once; a second is a no-op.
    assert :ok = Credits.release(tab, token)
    assert :ok = Credits.release(tab, token)
    assert Credits.snapshot(tab).general == {0, 26}
  end

  test "a stale token cannot free or claim a recycled slot" do
    tab = Credits.new()
    {:ok, token} = Credits.reserve(tab, :general)
    :ok = Credits.release(tab, token)

    {:ok, {n2, epoch2} = token2} = Credits.reserve(tab, :general)
    assert epoch2 != elem(token, 1)

    :ok = Credits.release(tab, token)
    assert Credits.snapshot(tab).general == {1, 26}
    assert :none = Credits.take(tab, n2, elem(token, 1))

    :ok = Credits.enqueue(tab, token2, payload())
    {:ok, pl} = Credits.take(tab, n2, epoch2)
    assert is_tuple(pl)
    :ok = Credits.finish(tab, token2)
    assert Credits.snapshot(tab).general == {0, 26}
  end

  test "a stale token's enqueue cannot overwrite a recycled slot's row" do
    tab = Credits.new()
    {:ok, {n, epoch} = stale} = Credits.reserve(tab, :general)
    :ok = Credits.release(tab, {n, epoch})

    # Slot n is free and lowest — the next reserve recycles it.
    {:ok, {^n, epoch2}} = Credits.reserve(tab, :general)
    assert epoch2 != epoch

    # The stale holder's late enqueue must fail the CAS — no write.
    assert {:error, :lost} = Credits.enqueue(tab, stale, payload())

    # The fresh holder's reserved row is untouched.
    assert [{^n, :reserved, :general, pid, ^epoch2, 0, nil}] = :ets.lookup(tab, n)
    assert pid == self()
    assert Credits.snapshot(tab).enqueued == 0
  end

  test "enqueue is holder-pinned — a foreign pid cannot publish the token" do
    tab = Credits.new()
    {:ok, token} = Credits.reserve(tab, :general)
    test_pid = self()

    {_pid, ref} =
      spawn_monitor(fn ->
        send(test_pid, {:foreign, Credits.enqueue(tab, token, payload())})
      end)

    wait_down(ref)

    assert_received {:foreign, {:error, :lost}}
    assert Credits.snapshot(tab).general == {1, 26}
    assert Credits.snapshot(tab).enqueued == 0

    # The real holder still publishes normally.
    assert :ok = Credits.enqueue(tab, token, payload())
  end

  test "concurrent reserves never over-book and epochs are unique" do
    tab = Credits.new()
    n = 40

    results =
      1..n
      |> Enum.map(fn _ -> Task.async(fn -> Credits.reserve(tab, :general) end) end)
      |> Enum.map(&Task.await/1)

    oks = Enum.filter(results, &match?({:ok, _}, &1))
    exhausted = Enum.filter(results, &(&1 == {:error, :exhausted}))

    assert length(oks) == 26
    assert length(exhausted) == n - 26
    assert length(Enum.uniq(Enum.map(oks, &elem(elem(&1, 1), 1)))) == 26
    assert Credits.snapshot(tab).general == {26, 26}
  end

  test "owner table death turns caller-side ops into typed values, never crashes" do
    tab = Credits.new()
    {:ok, token} = Credits.reserve(tab, :general)
    :ets.delete(tab)

    assert {:error, :no_table} = Credits.reserve(tab, :general)
    assert {:error, :no_table} = Credits.enqueue(tab, token, payload())
    assert :ok = Credits.release(tab, token)
    assert :ok = Credits.finish(tab, token)
    assert :none = Credits.take(tab, elem(token, 0), elem(token, 1))
    assert [] = Credits.enqueued(tab)
    assert 0 = Credits.sweep(tab)
    assert %{general: {0, 26}} = Credits.snapshot(tab)
  end

  test "high-water: 32 live slots is the ceiling including internal" do
    tab = Credits.new()

    tokens =
      Enum.map(1..26, fn _ -> elem(Credits.reserve(tab, :general), 1) end) ++
        Enum.map(1..6, fn _ -> elem(Credits.reserve(tab, :internal), 1) end)

    assert {:error, :exhausted} = Credits.reserve(tab, :general)
    assert {:error, :exhausted} = Credits.reserve(tab, :internal)

    s = Credits.snapshot(tab)
    assert s.general == {26, 26} and s.internal == {6, 6}
    assert length(tokens) == 32
  end
end
