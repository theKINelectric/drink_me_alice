defmodule DrinkMeAlice.Outcome do
  @moduledoc """
  Classifies raw SurrealDB replies for possibly-submitted mutations and
  reads into *facts*.

  The core rule: once a request may have entered the write path, a lost
  or malformed reply is `{:unknown, request}` — never silently `rejected`
  or `committed`. `{:refused, …}` facts exist only where the server gave
  *qualified* evidence the request could not have executed through a
  request-level or session-level RPC refusal. Incomplete replies remain
  inconclusive.

  SurrealDB 3.0.5 statement-result semantics used here (pinned
  `dbs/executor.rs:675-1035` — review-44 correction):

  - A `query` reply's `result` is a statement array. COMMIT has its own
    slot ONLY on success: the inner executor pushes the COMMIT result
    after `txn.commit()` returns ok (~:917), and consumes COMMIT with
    NO result entry on every failure path. A failed reply is therefore
    one entry SHORTER than the script — the trailing `RETURN` executes
    at the nominal COMMIT index. The classifier keys on the expected
    total length (`ctx.len` from `DrinkMeAlice.Tx`), never on the
    COMMIT index alone.
  - Full-length reply (`len == ctx.len`): `results[ci]` is the real
    COMMIT slot. `OK` is observed commit success — a post-commit error
    cannot un-commit it, so a missing/failed decision is
    `:committed_no_decision`, never a false rejection. Any `ERR` at
    `ci` in a full-length array is an unqualified shape (the pinned
    executor never pushes a COMMIT error) — inconclusive.
  - Shortened reply (`len == ctx.len - 1`): the COMMIT entry was
    omitted; `results[ci]` is the *shifted RETURN* and is NEVER commit
    evidence. Qualified shapes over the in-tx entries at 1..ci-1
    (statement errors carry `kind`/`details` at ENTRY level with
    `result` a message STRING — pinned `dbs/response.rs:78-90,113-164`,
    confirmed by the live capture `memory-reply-shapes-live-02.txt`):
      * statement abort: BEGIN `{status: "OK", result: nil}` at 0
        (outside the rewrite's `[start_results..]` range), entries
        before the failing statement are `NotExecuted` markers
        (`{status: "ERR", kind: "Query", details: {kind: "NotExecuted"},
        result: <string>}`), the failing statement carries the real
        error (a non-marker `ERR` with a message-string result), and
        every statement after it is a `Cancelled` marker — exactly ONE
        real error, marker order NotExecuted-before / Cancelled-after →
        definite non-commit (`{:rejected, _}`).
      * homogeneous cancellation: ALL in-tx entries are `Cancelled`
        markers (ctx-done path) or ALL are `NotExecuted` markers
        (tx-creation failure) — definite non-commit. Mixed marker sets
        are not produced → unqualified.
    A shortened reply needs ≥2 in-tx entries (ci ≥ 3) for the
    single-error abort qualification: with one in-tx statement the
    commit-failure rewrite produces exactly one `Internal` error —
    indistinguishable from a lone abort error without message
    substrings.
  - Commit failure (`txn.commit()` error, ~:930): every in-tx entry is
    rewritten to `Internal("Query not executed: {e}")` — MULTIPLE real
    errors — and `surrealkv/mod.rs` commits the storage transaction
    before `commit_coordinator.wait_for_sync`, so the write may already
    be durable. No rollback certificate exists on this wire shape —
    always inconclusive. Earlier "real" errors in that reply are
    rewrites, not abort causes.
  - A full-length reply must have `OK` status on every pre-COMMIT
    entry, `result: nil` (NONE) at BEGIN and COMMIT — a contradictory
    pre-commit error or a non-nil COMMIT cannot establish success.
  - `THROW` surfaces as entry-level `kind: "Thrown"` with the thrown
    tag inside the message-string `result`.
  - Any other length, an `OK` mid-tx entry inside a shortened reply, a
    non-OK BEGIN, or any silhouette not listed above is unsupported —
    inconclusive. A shifted `RETURN` is never read as COMMIT success.

  Every statement entry must match the `{status: "OK"|"ERR", result: _}`
  shape — unknown statuses, scalars or malformed entries make the whole
  reply inconclusive rather than letting a valid-looking decision
  overshadow corrupt neighbours.
  """

  alias DrinkMeAlice.Error

  @type fact ::
          {:committed, map()}
          | {:existing, map()}
          | {:rejected, Error.t()}
          | {:session_refused}
          | {:refused, integer()}
          | {:inconclusive, term()}

  # Pinned 3.0.5 wire codes (surrealdb/types/src/error.rs + rpc/error.rs):
  #
  #   -32700 Parse / -32600 InvalidRequest / -32601 MethodNotFound /
  #   -32602 MethodNotAllowed / -32603 InvalidParams — request-level
  #   refusals that provably precede execution.
  #   -32002 InvalidAuth — the auth check itself refused: session-level,
  #   and the request never ran.
  #   -32003 QueryNotExecuted is NOT a certified refusal (review-43):
  #   `dbs/executor.rs` raises it for a failed implicit commit — i.e.
  #   after the transaction already ran — so it proves nothing about
  #   execution and stays inconclusive.
  #   -32000 Internal is GENERIC — SessionExpired shares it with
  #   unrelated internal failures. Only the one wrapper chain the pinned
  #   server actually emits for an expired session qualifies
  #   (`NotAllowed` → `Auth` → `SessionExpired`, review-12/13); every
  #   other -32000 keeps a possibly-submitted mutation inconclusive,
  #   never certified refused.
  @auth_refusal_code -32002
  @preexec_codes [-32700, -32600, -32601, -32602, -32603]
  @session_expired_kind "SessionExpired"

  @doc """
  Classify a mutation reply.

  `verdicts` maps the script's decision strings to outcome atoms used by
  the caller. `ctx` carries `%{commit: i, decision: j, len: n}` — the
  nominal COMMIT index, the trailing RETURN index, and the script's
  statement count, emitted by `DrinkMeAlice.Tx` builders (`len`
  defaults to `j + 1`: RETURN is the last statement). When `ctx` is
  absent (tests, migrations-side replies), the strict fallback runs:
  a decision is honoured only if *no* entry carries any error and the
  complete array is well-shaped.
  """
  @spec classify_tx(map(), map(), map() | nil) :: fact()
  def classify_tx(reply, verdicts, ctx \\ nil)

  def classify_tx(%{"error" => %{"code" => code} = err}, _v, _ctx)
      when is_integer(code) do
    cond do
      session_refusal?(err) -> {:session_refused}
      code in @preexec_codes -> {:refused, code}
      true -> {:inconclusive, {:rpc_error, code}}
    end
  end

  def classify_tx(%{"error" => other}, _v, _ctx), do: {:inconclusive, {:rpc_error, other}}

  def classify_tx(%{"result" => results}, verdicts, ctx) when is_list(results) do
    case valid_statements(results) do
      :ok ->
        case ctx do
          %{commit: ci, decision: di} when is_integer(ci) and is_integer(di) ->
            classify_positional(results, ctx, verdicts)

          _ ->
            classify_strict(results, verdicts)
        end

      {:error, why} ->
        {:inconclusive, why}
    end
  end

  def classify_tx(_other, _v, _ctx), do: {:inconclusive, :bad_reply_shape}

  @doc """
  Classify a read reply — single `RETURN` statement. Returns
  `{:ok, map}` | `{:error, :session | :rpc_error | :bad_shape}`.
  """
  @spec classify_read(map()) :: {:ok, map()} | {:error, atom()}
  def classify_read(%{"error" => %{"code" => code} = err}) when is_integer(code) do
    if session_refusal?(err), do: {:error, :session}, else: {:error, :rpc_error}
  end

  def classify_read(%{"error" => _}), do: {:error, :rpc_error}

  def classify_read(%{"result" => results}) when is_list(results) do
    case valid_statements(results) do
      :ok ->
        Enum.find_value(results, {:error, :bad_shape}, fn
          %{"status" => "OK", "result" => %{} = m} -> {:ok, m}
          %{"status" => "ERR", "result" => r} -> {:error, read_err(r)}
          _ -> false
        end)

      {:error, _} ->
        {:error, :bad_shape}
    end
  end

  def classify_read(_), do: {:error, :bad_shape}

  @doc """
  True when the wire error object is a session refusal: `-32002`
  InvalidAuth outright, or `-32000` carrying the exact expired-session
  wrapper chain the pinned 3.0.5 server emits — `kind: "NotAllowed"`,
  `details.kind: "Auth"`, `details.details.kind: "SessionExpired"`.
  A `SessionExpired` kind under any other wrapper, a malformed chain,
  or a bare `-32000` is generic internal evidence: NOT a session
  refusal (review-13).
  """
  @spec session_refusal?(map()) :: boolean()
  def session_refusal?(%{"code" => @auth_refusal_code}), do: true

  def session_refusal?(%{"code" => -32000} = err), do: expired_session_shape?(err)

  def session_refusal?(_), do: false

  # Qualified chain only — NotAllowed → Auth → SessionExpired — exactly
  # as captured against the pinned server (review-12). Additional fields
  # alongside the chain are fine; any deviation in the kind sequence is
  # inconclusive evidence, not a refusal.
  defp expired_session_shape?(%{
         "kind" => "NotAllowed",
         "details" => %{
           "kind" => "Auth",
           "details" => %{"kind" => @session_expired_kind}
         }
       }),
       do: true

  defp expired_session_shape?(_), do: false

  @doc """
  True when the error CODE ALONE proves a session refusal — only
  `-32002`. `-32000` requires structured evidence; use
  `session_refusal?/1` on the whole error object.
  """
  @spec session_code?(integer()) :: boolean()
  def session_code?(code), do: code == @auth_refusal_code

  # -- internals ---------------------------------------------------------------

  defp valid_statements(results) do
    if Enum.all?(results, &statement_shape?/1), do: :ok, else: {:error, :bad_statement}
  end

  defp statement_shape?(%{"status" => s, "result" => _}) when s in ["OK", "ERR"], do: true
  defp statement_shape?(_), do: false

  # Positional: the pinned executor pushes a COMMIT result entry ONLY on
  # success; every failure path consumes COMMIT silently, so a failed
  # reply is one entry shorter and the trailing RETURN lands at the
  # nominal COMMIT index. The expected total length decides which
  # interpretation applies — never the COMMIT index alone.
  defp classify_positional(results, %{commit: ci, decision: di} = ctx, verdicts) do
    len = Map.get(ctx, :len, di + 1)

    if ci >= 1 and di > ci and len > di do
      case length(results) do
        ^len -> classify_full(results, ci, di, verdicts)
        n when n == len - 1 -> classify_shortened(results, ci)
        n -> {:inconclusive, {:unexpected_result_count, n, len}}
      end
    else
      {:inconclusive, :bad_ctx}
    end
  end

  # Full-length: every pre-COMMIT entry must be a success (a real in-tx
  # error would have aborted and shortened the reply — an ERR here is
  # contradictory), BEGIN and COMMIT must be `OK` with a nil (NONE)
  # result — the pinned executor never pushes a COMMIT error, and a
  # non-nil COMMIT value cannot establish success. Only then is
  # results[ci] observed commit success; a post-commit error can never
  # undo it.
  defp classify_full(results, ci, di, verdicts) do
    begin = Enum.at(results, 0)
    commit = Enum.at(results, ci)
    pre_in_tx = Enum.slice(results, 1, ci - 1)

    cond do
      not match?(%{"status" => "OK", "result" => nil}, begin) ->
        {:inconclusive, :malformed_begin}

      Enum.any?(pre_in_tx, &(not match?(%{"status" => "OK"}, &1))) ->
        {:inconclusive, :contradictory_precommit_errors}

      match?(%{"status" => "OK", "result" => nil}, commit) ->
        case decision_at(results, di) do
          {:ok, decision, map} -> verdict_to_fact(decision, verdicts, map)
          :error -> {:inconclusive, :committed_no_decision}
        end

      true ->
        {:inconclusive, {:commit_result_unknown, error_message(commit)}}
    end
  end

  # Shortened: the COMMIT entry was omitted; results[ci] is the shifted
  # RETURN — never commit evidence. Qualified silhouettes over the in-tx
  # entries at 1..ci-1 (statement errors carry `kind`/`details` at entry
  # level with `result` a message string — response.rs:78-90,113-164):
  #
  #   * exactly one real error, every entry before it a NotExecuted
  #     marker, every entry after it a Cancelled marker → statement
  #     abort; that error is the cause. Requires ≥2 in-tx entries
  #     (ci ≥ 3): with one, the commit-failure rewrite emits exactly one
  #     Internal error — indistinguishable without message substrings.
  #   * all Cancelled markers, or all NotExecuted markers → cancelled /
  #     never-created tx — definite non-commit. Mixed marker sets are
  #     not produced by the executor → unqualified.
  #
  # Multiple real errors = the commit-failure Internal rewrite (or the
  # missing-COMMIT rewrite) — inconclusive; the storage commit may be
  # durable. Anything else (OK entries mid-tx, an ERR without a message
  # string, bad BEGIN, ci < 2) is an unsupported silhouette —
  # inconclusive.
  defp classify_shortened(results, ci) do
    in_tx = Enum.slice(results, 1, ci - 1)

    real_idx =
      in_tx
      |> Enum.with_index()
      |> Enum.filter(fn {e, _i} -> real_error?(e) end)
      |> Enum.map(&elem(&1, 1))

    cond do
      ci < 2 or not match?(%{"status" => "OK", "result" => nil}, Enum.at(results, 0)) ->
        {:inconclusive, :shortened_shape_unqualified}

      length(real_idx) > 1 ->
        {:inconclusive, {:commit_result_unknown, error_message(Enum.at(in_tx, hd(real_idx)))}}

      real_idx == [] and homogeneous_markers?(in_tx) ->
        aborted_fact(hd(in_tx))

      length(real_idx) == 1 and ci >= 3 and ordered_markers?(in_tx, hd(real_idx)) ->
        aborted_fact(Enum.at(in_tx, hd(real_idx)))

      true ->
        {:inconclusive, :shortened_shape_unqualified}
    end
  end

  # All-marker region is homogeneous: all Cancelled (ctx-done cancel
  # path) or all NotExecuted (tx-creation failure path) — the two
  # producer paths never mix markers.
  defp homogeneous_markers?(in_tx) do
    in_tx != [] and
      (Enum.all?(in_tx, &marker?(&1, "Cancelled")) or
         Enum.all?(in_tx, &marker?(&1, "NotExecuted")))
  end

  # Abort marker order per the pinned executor: NotExecuted strictly
  # before the real error (rewritten priors), Cancelled strictly after
  # (drained statements).
  defp ordered_markers?(in_tx, j) do
    Enum.all?(Enum.take(in_tx, j), &marker?(&1, "NotExecuted")) and
      Enum.all?(Enum.drop(in_tx, j + 1), &marker?(&1, "Cancelled"))
  end

  # Strict fallback without position context: honour a decision only when
  # every entry is well-shaped and no entry carries an error.
  defp classify_strict(results, verdicts) do
    if Enum.any?(results, &match?(%{"status" => "ERR"}, &1)) do
      {:inconclusive, :statement_errors}
    else
      case List.last(results) do
        %{"status" => "OK", "result" => %{"decision" => d} = map} when is_binary(d) ->
          verdict_to_fact(d, verdicts, map)

        _ ->
          {:inconclusive, :no_decision}
      end
    end
  end

  defp decision_at(results, di) do
    case Enum.at(results, di, :missing) do
      %{"status" => "OK", "result" => %{"decision" => d} = map} when is_binary(d) ->
        {:ok, d, map}

      _ ->
        :error
    end
  end

  # `verdicts` maps each decision string to `:committed` | `:existing` |
  # `{:rejected, code}` | `{:rejected, code, detail}`. Committed and
  # existing facts carry the whole decision map so the caller can decode
  # the authoritative record out of it.
  defp verdict_to_fact(decision, verdicts, map) do
    case Map.fetch(verdicts, decision) do
      {:ok, :committed} -> {:committed, map}
      {:ok, :existing} -> {:existing, map}
      {:ok, {:rejected, code}} -> {:rejected, Error.new(code, {:decision, decision})}
      {:ok, {:rejected, code, detail}} -> {:rejected, Error.new(code, detail)}
      {:ok, other} -> {:inconclusive, {:bad_verdict, other}}
      :error -> {:inconclusive, {:unmapped_decision, decision}}
    end
  end

  # A skipped/rolled-back statement marker — entry-level `kind: "Query"`
  # + `details.kind` of NotExecuted/Cancelled with a message-string
  # result (response.rs query-result wire shape). Never the abort cause.
  defp marker?(%{"status" => "ERR", "kind" => "Query", "result" => r} = e, k)
       when is_binary(r),
       do: match?(%{"details" => %{"kind" => ^k}}, e)

  defp marker?(_, _), do: false

  # A real (non-marker) statement error: `ERR` status with a
  # message-string result. An ERR whose result is not a string is
  # neither marker nor valid cause — it leaves the silhouette
  # unqualified.
  defp real_error?(%{"status" => "ERR", "result" => r} = e) when is_binary(r),
    do: not marker?(e, "NotExecuted") and not marker?(e, "Cancelled")

  defp real_error?(_), do: false

  # An aborted-by-real-error fact: structured evidence the tx did not
  # commit → definite non-execution. The cause (constraint, type error,
  # THROW tag) is an in-script failure — an `internal_error` rejection
  # carrying the real message, never a placeholder's text.
  defp aborted_fact(entry) do
    {:rejected, Error.new("internal_error", {:tx_aborted, error_message(entry)})}
  end

  defp error_message(%{"result" => r}) when is_binary(r), do: r
  defp error_message(e), do: inspect(e)

  defp read_err(_), do: :statement_error
end
