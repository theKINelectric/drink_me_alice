if Code.ensure_loaded?(Ash.DataLayer) do
  defmodule DrinkMeAlice.Ash.OutcomeUnknown do
    @moduledoc """
    A write that may or may not have committed: the request reached the
    wire but no classifiable reply came back (lost reply, timeout,
    inconclusive reply shape). This is NOT a failure — reconcile by
    reading the record, or by retrying the identical write (creates are
    idempotent on the primary key: a replayed create that already landed
    is refused as "already taken", which proves it committed).
    """
    use Splode.Error, fields: [:resource, :action_type, :primary_key], class: :unknown

    def message(e) do
      "outcome unknown for #{e.action_type} on #{inspect(e.resource)} " <>
        "#{inspect(e.primary_key)} — the write may have committed; reconcile by reading"
    end
  end

  defmodule DrinkMeAlice.Ash.Unavailable do
    @moduledoc """
    The store refused before the request was sent (credit bound,
    connection down, deadline passed while queued, session refused,
    clock uncertainty) — certified not submitted, safe to retry.
    """
    use Splode.Error, fields: [:resource, :reason], class: :unknown

    def message(e), do: "SurrealDB unavailable for #{inspect(e.resource)}: #{inspect(e.reason)}"
  end

  defmodule DrinkMeAlice.Ash.QueryFailed do
    @moduledoc "A read or write the store rejected, or a bound this data layer enforces."
    use Splode.Error, fields: [:resource, :reason], class: :invalid

    def message(e), do: "SurrealDB query failed for #{inspect(e.resource)}: #{inspect(e.reason)}"
  end
end
