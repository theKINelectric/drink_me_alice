defmodule DrinkMeAlice.Credits do
  @moduledoc """
  The 32-credit RPC admission bound — a fixed slot table whose rows also
  hold the bounded request payload.

  Design (post-review): the request is *stored in the slot row*, not
  carried through the owner mailbox. The owner drains `:enqueued` rows
  on a fixed poll — there is no caller→owner message at all, so the
  mailbox bound does not depend on caller timing (a delayed doorbell
  sender can inject a stale message at an arbitrary later time; a
  counter edge cannot bound that — polling can).

  - `reserve/2`: one atomic `select_replace` CAS claims a free slot —
    `{n, :free, class, 0, 0, 0, nil}` → `{n, :reserved, class, pid,
    epoch, 0, nil}`. `epoch` is a unique reservation identity; every
    later transition matches on `{n, epoch}` so no release can be
    replayed or land on a recycled slot.
  - `enqueue/3`: ONE `select_replace` CAS — `reserved → enqueued` —
    that matches `{n, :reserved, _, self(), epoch, _, nil}` and installs
    the payload via `{:const, payload}` in the same write. Payload
    installation and state publication are atomic: a `reserved` row can
    never hold a payload, so a holder that dies after `reserve` leaves
    a sweepable `reserved`/nil row and nothing more. A stale token
    cannot match — the epoch pin fails on any recycled slot, so no
    write can stomp another slot lifetime.
  - `take/3`: owner CAS `enqueued → processing` returning the payload.
    `finish/2`: owner CAS `processing → free`. The slot's credit
    lifetime covers the entire queued-and-processing window — including
    caller death or caller timeout — because freeing is the owner's act
    of consuming the row, not the caller's.
  - `release/2`: caller-side free, used only when the reservation was
    provably never enqueued (owner unresolvable before `enqueue`).
  - `sweep/1`: frees `:reserved` rows whose holder pid is dead (died
    between reserve and enqueue — no payload exists to orphan).
    `:enqueued`/`:processing` rows are *never* swept — they always carry
    a real payload owned by the live owner; if the owner dies the whole
    table dies with it.

  Slot keys are flat integers (1..26 general, 27..32 internal) because
  `ets:select_replace` requires a literal non-compound key in the match
  head — `{n, …}` keeps every transition a single-key CAS.

  Slot bound: exactly 32 reserved requests, total.

  Dead-table handling: an `ArgumentError` from ETS is typed as
  `:no_table`/empty/zero **only when the table is actually gone**
  (`:ets.info` → `:undefined`). On a live table the error is re-raised —
  a malformed match spec is a programming defect, not a dead table, and
  must crash loudly in tests rather than masquerade as `:no_table`.
  """

  @general 26
  @internal 6

  @type class :: :general | :internal
  @type token :: {non_neg_integer(), non_neg_integer()}
  @type payload :: {reference(), map(), integer()}

  @doc "The declared split for evidence/tests."
  @spec bounds() :: map()
  def bounds, do: %{total: @general + @internal, general: @general, internal: @internal}

  @doc "Create the slot table (all slots `:free`). Owned by the caller."
  @spec new() :: :ets.tid()
  def new do
    tab =
      :ets.new(__MODULE__, [:public, :set, {:read_concurrency, true}, {:write_concurrency, true}])

    :ets.insert(tab, {:epoch, 0})

    for n <- 1..(@general + @internal) do
      :ets.insert(tab, {n, :free, class_of(n), 0, 0, 0, nil})
    end

    tab
  end

  @doc """
  Atomically claim a free slot of `class` for the calling pid.
  `{:ok, {n, epoch}}` | `{:error, :exhausted | :no_table}`.
  """
  @spec reserve(:ets.tid(), class()) :: {:ok, token()} | {:error, :exhausted | :no_table}
  def reserve(tab, class) when class in [:general, :internal] do
    pid = self()
    epoch = bump(tab)

    class
    |> slot_ids()
    |> Enum.reduce_while({:error, :exhausted}, fn n, _acc ->
      case :ets.select_replace(
             tab,
             [
               {{n, :free, class, 0, 0, 0, nil}, [],
                [{{n, :reserved, class, pid, epoch, 0, nil}}]}
             ]
           ) do
        1 -> {:halt, {:ok, {n, epoch}}}
        0 -> {:cont, {:error, :exhausted}}
      end
    end)
  rescue
    e in ArgumentError ->
      if dead?(tab), do: {:error, :no_table}, else: reraise(e, __STACKTRACE__)
  end

  @doc """
  Publish the request payload and flip `reserved → enqueued` in ONE
  `select_replace` CAS. `:ok` | `{:error, :lost | :no_table}`.

  The match requires `{n, :reserved, _, self(), epoch, _, nil}` — the
  payload rides in the spec as `{:const, payload}`, so installation and
  publication are a single atomic write. There is no interim
  reserved-with-payload state: a holder that dies before the CAS leaves
  an ordinary sweepable `reserved`/nil row; a stale token on a recycled
  slot fails the epoch match and cannot overwrite anything.
  """
  @spec enqueue(:ets.tid(), token(), payload()) :: :ok | {:error, :lost | :no_table}
  def enqueue(tab, {n, epoch}, {_alias, _request, _deadline} = payload) do
    pid = self()
    now = System.monotonic_time(:millisecond)

    case :ets.select_replace(
           tab,
           [
             {{n, :reserved, :"$1", pid, epoch, :_, nil}, [],
              [{{n, :enqueued, :"$1", pid, epoch, now, {:const, payload}}}]}
           ]
         ) do
      1 -> :ok
      0 -> {:error, :lost}
    end
  rescue
    e in ArgumentError ->
      if dead?(tab), do: {:error, :no_table}, else: reraise(e, __STACKTRACE__)
  end

  @doc """
  Caller-side free of a reservation that was never enqueued — used when
  the owner could not be resolved. Epoch- and holder-pinned; at most once.
  """
  @spec release(:ets.tid(), token()) :: :ok
  def release(tab, {n, epoch}) do
    _ =
      :ets.select_replace(
        tab,
        [{{n, :reserved, :"$1", self(), epoch, 0, nil}, [], [{{n, :free, :"$1", 0, 0, 0, nil}}]}]
      )

    :ok
  rescue
    e in ArgumentError ->
      if dead?(tab), do: :ok, else: reraise(e, __STACKTRACE__)
  end

  # -- owner side ---------------------------------------------------------------

  @doc """
  List currently `:enqueued` slots, oldest enqueue first
  (`{n, epoch, marked_ms, payload}`). Read-only view for the owner's
  eligibility scan; consumption is still the CAS in `take/3`.
  """
  @spec enqueued(:ets.tid()) :: [{non_neg_integer(), non_neg_integer(), integer(), payload()}]
  def enqueued(tab) do
    tab
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {n, :enqueued, _class, _pid, epoch, ms, payload} -> [{n, epoch, ms, payload}]
      _ -> []
    end)
    |> Enum.sort_by(fn {_n, _e, ms, _pl} -> ms end)
  rescue
    e in ArgumentError ->
      if dead?(tab), do: [], else: reraise(e, __STACKTRACE__)
  end

  @doc """
  Atomically claim one enqueued slot for processing —
  `enqueued → processing` in one CAS, returning the stored payload.
  `{:ok, payload}` | `:none`.
  """
  @spec take(:ets.tid(), non_neg_integer(), non_neg_integer()) :: {:ok, payload()} | :none
  def take(tab, n, epoch) do
    # CAS preserving class, marked_ms and payload:
    # {n,:enqueued,c,_,epoch,ms,pl} → {n,:processing,c,self(),epoch,ms,pl}.
    # The epoch pin makes a stale take on a recycled slot impossible.
    case :ets.select_replace(
           tab,
           [
             {{n, :enqueued, :"$1", :_, epoch, :"$2", :"$3"}, [],
              [{{n, :processing, :"$1", self(), epoch, :"$2", :"$3"}}]}
           ]
         ) do
      0 ->
        :none

      1 ->
        case :ets.lookup(tab, n) do
          [{^n, :processing, _class, _owner, ^epoch, _ms, payload}] -> {:ok, payload}
          _ -> :none
        end
    end
  rescue
    e in ArgumentError ->
      if dead?(tab), do: :none, else: reraise(e, __STACKTRACE__)
  end

  @doc """
  Owner-side free once the request is consumed (replied or refused).
  Matched on `{n, epoch}` — frees `:processing` (normal) and, defensively,
  `:enqueued` rows so a slot listed twice across a poll boundary still
  drains to `:free` exactly once.
  """
  @spec finish(:ets.tid(), token()) :: :ok
  def finish(tab, {n, epoch}) do
    _ =
      :ets.select_replace(
        tab,
        [
          {{n, :processing, :"$1", :_, epoch, :_, :_}, [], [{{n, :free, :"$1", 0, 0, 0, nil}}]},
          {{n, :enqueued, :"$1", :_, epoch, :_, :_}, [], [{{n, :free, :"$1", 0, 0, 0, nil}}]}
        ]
      )

    :ok
  rescue
    e in ArgumentError ->
      if dead?(tab), do: :ok, else: reraise(e, __STACKTRACE__)
  end

  @doc """
  Reclaim `:reserved` slots whose holder died between reserve and
  enqueue — the only state without an installed payload. `:enqueued` and
  `:processing` rows are left to the owner, which always consumes them.
  Returns the number reclaimed.
  """
  @spec sweep(:ets.tid()) :: non_neg_integer()
  def sweep(tab) do
    tab
    |> :ets.tab2list()
    |> Enum.reduce(0, fn
      {n, :reserved, class, pid, epoch, _ms, nil}, acc when is_pid(pid) ->
        if Process.alive?(pid) do
          acc
        else
          acc +
            :ets.select_replace(
              tab,
              [
                {{n, :reserved, class, pid, epoch, :_, nil}, [],
                 [{{n, :free, class, 0, 0, 0, nil}}]}
              ]
            )
        end

      _other, acc ->
        acc
    end)
  rescue
    e in ArgumentError ->
      if dead?(tab), do: 0, else: reraise(e, __STACKTRACE__)
  end

  @doc "Current per-class usage and queue-state counts — for health/evidence."
  @spec snapshot(:ets.tid()) :: map()
  def snapshot(tab) do
    rows = :ets.tab2list(tab)

    count = fn class ->
      Enum.count(rows, fn
        {_n, s, ^class, _, _, _, _} when s != :free -> true
        _ -> false
      end)
    end

    state = fn s -> Enum.count(rows, &match?({_n, ^s, _c, _p, _e, _m, _pl}, &1)) end

    epoch =
      try do
        :ets.lookup_element(tab, :epoch, 2)
      rescue
        ArgumentError -> 0
      end

    %{
      general: {count.(:general), @general},
      internal: {count.(:internal), @internal},
      enqueued: state.(:enqueued),
      processing: state.(:processing),
      epoch: epoch
    }
  rescue
    e in ArgumentError ->
      if dead?(tab) do
        %{general: {0, @general}, internal: {0, @internal}, enqueued: 0, processing: 0, epoch: 0}
      else
        reraise(e, __STACKTRACE__)
      end
  end

  # -- internals ----------------------------------------------------------------

  defp dead?(tab), do: :ets.info(tab) == :undefined

  defp bump(tab), do: :ets.update_counter(tab, :epoch, {2, 1}, {:epoch, 0})

  defp slot_ids(:general), do: 1..@general
  defp slot_ids(:internal), do: (@general + 1)..(@general + @internal)

  defp class_of(n) when n <= @general, do: :general
  defp class_of(_n), do: :internal
end
