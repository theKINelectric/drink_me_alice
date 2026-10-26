defmodule DrinkMeAlice.OutcomeTest do
  @moduledoc """
  Regression coverage for the outcome classifier against the five
  failures root reproduced in early-review-02:

  1. Unknown statement statuses must not let a valid decision commit.
  2. Scalar (non-map) statement elements must not let a decision commit.
  3. Non-string `ERR` results / non-string decisions must not raise or
     commit — malformed evidence is inconclusive.
  4. Unqualified RPC error codes (e.g. -32999) prove nothing about
     execution — inconclusive, never a definite rejection.
  5. `NotExecuted`/`Cancelled` placeholders must not hide the real abort
     cause elsewhere in the array, and a post-commit error must not
     un-commit an observed-OK COMMIT slot.
  """

  use ExUnit.Case, async: true

  alias DrinkMeAlice.Error
  alias DrinkMeAlice.Outcome

  @verdicts %{
    "commit" => :committed,
    "replay" => :committed,
    "conflict" => {:rejected, "operation_id_conflict"},
    "stale" => {:rejected, "stale_attempt"},
    "full" => {:rejected, "backlog_full"}
  }

  # A minimal 3-statement tx shape: BEGIN(0), COMMIT(1), RETURN(2).
  @ctx %{commit: 1, decision: 2}

  defp ok(result), do: %{"status" => "OK", "result" => result}

  # Real pinned-3.0.5 wire error shape (dbs/response.rs:78-90,113-164 +
  # live capture memory-reply-shapes-live-02): `kind`/`details` at ENTRY
  # level, `result` is the message STRING.
  defp real_err(kind, msg), do: %{"status" => "ERR", "kind" => kind, "result" => msg}

  defp marker(kind),
    do: %{
      "status" => "ERR",
      "kind" => "Query",
      "details" => %{"kind" => kind},
      "result" => "The query was not executed"
    }

  defp not_executed, do: marker("NotExecuted")
  defp cancelled, do: marker("Cancelled")

  defp rewrite(msg \\ "Query not executed: grouped WAL sync failed"),
    do: real_err("Internal", msg)

  defp decision(d),
    do: %{
      "decision" => d,
      "op" => nil,
      "attempts" => [],
      "now" => "t",
      "floor" => "t",
      "regressed" => false
    }

  test "clean commit path maps the decision" do
    reply = %{"result" => [ok(nil), ok(nil), ok(decision("commit"))]}
    assert {:committed, %{"decision" => "commit"}} = Outcome.classify_tx(reply, @verdicts, @ctx)
  end

  test "unknown statement status anywhere makes the reply inconclusive" do
    reply = %{
      "result" => [
        ok(nil),
        ok(nil),
        %{"status" => "PENDING", "result" => nil},
        ok(decision("commit"))
      ]
    }

    assert {:inconclusive, _} = Outcome.classify_tx(reply, @verdicts, @ctx)
  end

  test "scalar statement elements make the reply inconclusive" do
    reply = %{"result" => [ok(nil), ok(nil), 42, ok(decision("commit"))]}
    assert {:inconclusive, _} = Outcome.classify_tx(reply, @verdicts, @ctx)
  end

  test "non-map ERR results are validated, not silently ignored" do
    # A real ERR at the COMMIT slot of a full-length reply is
    # unqualified — the pinned executor never pushes a COMMIT error —
    # so it is inconclusive, not a definite commit failure.
    reply = %{"result" => [ok(nil), real_err("Internal", "db blew up"), ok(decision("commit"))]}

    assert {:inconclusive, {:commit_result_unknown, "db blew up"}} =
             Outcome.classify_tx(reply, @verdicts, @ctx)
  end

  test "non-string decision at the decision slot is inconclusive, never a crash" do
    reply = %{"result" => [ok(nil), ok(nil), ok(%{"decision" => 42})]}
    assert {:inconclusive, :committed_no_decision} = Outcome.classify_tx(reply, @verdicts, @ctx)
  end

  test "unqualified RPC error -32999 is inconclusive — it proves nothing about execution" do
    reply = %{"error" => %{"code" => -32999, "message" => "internal"}}
    assert {:inconclusive, {:rpc_error, -32999}} = Outcome.classify_tx(reply, @verdicts, @ctx)
  end

  test "-32002 InvalidAuth is a session refusal" do
    reply = %{"error" => %{"code" => -32002, "message" => "invalid auth"}}
    assert {:session_refused} = Outcome.classify_tx(reply, @verdicts, @ctx)
  end

  test "bare -32000 is GENERIC Internal — never a session refusal on code alone" do
    # Pinned source: -32000 covers SessionExpired AND unrelated internal
    # failures; without structured evidence a possibly-submitted
    # mutation is inconclusive, not refused.
    for err <- [
          %{"code" => -32000, "message" => "internal"},
          %{"code" => -32000, "message" => "internal", "kind" => "Internal"},
          %{
            "code" => -32000,
            "message" => "internal",
            "kind" => "Validation",
            "details" => %{"kind" => "ParseError"}
          },
          %{
            "code" => -32000,
            "message" => "internal",
            "kind" => "NotAllowed",
            "details" => %{"kind" => "TableFull"}
          }
        ] do
      reply = %{"error" => err}
      assert {:inconclusive, {:rpc_error, -32000}} = Outcome.classify_tx(reply, @verdicts, @ctx)
    end
  end

  test "-32000 carrying the REAL SessionExpired wrapper chain IS a session refusal" do
    # The actual pinned-3.0.5 wire shape (review-12 capture): kind
    # "NotAllowed" wrapping details.kind "Auth" wrapping
    # details.kind "SessionExpired".
    err = %{
      "code" => -32000,
      "kind" => "NotAllowed",
      "details" => %{"kind" => "Auth", "details" => %{"kind" => "SessionExpired"}},
      "cause" => nil,
      "message" => "The session has expired"
    }

    assert {:session_refused} = Outcome.classify_tx(%{"error" => err}, @verdicts, @ctx)
  end

  test "-32000 SessionExpired under any OTHER wrapper stays inconclusive" do
    # Review-13: the qualifier is the observed chain, not the leaf kind.
    # A SessionExpired leaf under unrelated or malformed wrappers — and
    # a message-only match — are all inconclusive.
    for err <- [
          # flat leaf kind, no NotAllowed/Auth wrapper
          %{"code" => -32000, "kind" => "SessionExpired", "message" => "session expired"},
          # correct leaf, wrong top wrapper
          %{
            "code" => -32000,
            "kind" => "Internal",
            "details" => %{"kind" => "Auth", "details" => %{"kind" => "SessionExpired"}}
          },
          # correct leaf, missing the Auth middle
          %{"code" => -32000, "kind" => "NotAllowed", "details" => %{"kind" => "SessionExpired"}},
          # correct leaf, wrong middle wrapper
          %{
            "code" => -32000,
            "kind" => "NotAllowed",
            "details" => %{"kind" => "Internal", "details" => %{"kind" => "SessionExpired"}}
          },
          # message matches, structure absent
          %{"code" => -32000, "message" => "The session has expired"},
          # malformed details (not a map / deeper truncation)
          %{"code" => -32000, "kind" => "NotAllowed", "details" => "Auth"},
          %{
            "code" => -32000,
            "kind" => "NotAllowed",
            "details" => %{"kind" => "Auth", "details" => nil}
          }
        ] do
      reply = %{"error" => err}
      assert {:inconclusive, {:rpc_error, -32000}} = Outcome.classify_tx(reply, @verdicts, @ctx)
    end
  end

  test "invalid-request codes are certified refusals" do
    for code <- [-32700, -32600, -32601, -32602, -32603] do
      reply = %{"error" => %{"code" => code, "message" => "refused"}}
      assert {:refused, ^code} = Outcome.classify_tx(reply, @verdicts, @ctx)
    end
  end

  test "-32003 QueryNotExecuted is inconclusive, not a certified refusal" do
    # Pinned executor raises QueryNotExecuted for a FAILED IMPLICIT
    # COMMIT (dbs/executor.rs — bail after the transaction ran), so the
    # code does not prove non-execution (review-43).
    reply = %{"error" => %{"code" => -32003, "message" => "failed transaction"}}

    assert {:inconclusive, {:rpc_error, -32003}} =
             Outcome.classify_tx(reply, @verdicts, @ctx)
  end

  test "classify_read mirrors the same session distinction" do
    assert {:error, :session} =
             Outcome.classify_read(%{"error" => %{"code" => -32002, "message" => "x"}})

    real_expired = %{
      "code" => -32000,
      "kind" => "NotAllowed",
      "details" => %{"kind" => "Auth", "details" => %{"kind" => "SessionExpired"}}
    }

    assert {:error, :session} = Outcome.classify_read(%{"error" => real_expired})

    assert {:error, :rpc_error} =
             Outcome.classify_read(%{"error" => %{"code" => -32000, "kind" => "SessionExpired"}})

    assert {:error, :rpc_error} =
             Outcome.classify_read(%{"error" => %{"code" => -32000, "message" => "internal"}})

    assert {:error, :rpc_error} =
             Outcome.classify_read(%{"error" => %{"code" => -32003, "message" => "not executed"}})
  end

  # Real pinned-3.0.5 shapes (dbs/executor.rs — review-44): a COMMIT
  # result entry is pushed ONLY on success; every failure path consumes
  # COMMIT with no entry, so a failed reply is one entry short and the
  # trailing RETURN lands at the nominal COMMIT index. Statement abort
  # rewrites earlier in-tx entries to NotExecuted, pushes the real error,
  # pushes Cancelled for later in-tx statements; commit failure rewrites
  # ALL in-tx entries to Internal("Query not executed: {e}").

  # Script: BEGIN@0, three in-tx @1..3, COMMIT@4, RETURN@5 — len 6;
  # failed replies arrive length 5 with RETURN shifted to index 4.
  @tx6 %{commit: 4, decision: 5, len: 6}

  test "real abort shape: NotExecuted priors + one real error + Cancelled, COMMIT omitted" do
    for shifted <- [ok(decision("commit")), rewrite("decision blew up")] do
      results = [
        ok(nil),
        not_executed(),
        real_err("Internal", "type coercion failed"),
        cancelled(),
        shifted
      ]

      assert {:rejected, %Error{code: "internal_error", detail: {:tx_aborted, msg}}} =
               Outcome.classify_tx(%{"result" => results}, @verdicts, @tx6),
             "shifted #{inspect(shifted)}"

      assert msg =~ "type coercion"
    end
  end

  test "real THROW abort shape: definite rejection with the thrown tag" do
    results = [
      ok(nil),
      not_executed(),
      real_err("Thrown", "An error occurred: bf:backlog_underflow"),
      cancelled(),
      ok(decision("commit"))
    ]

    assert {:rejected, %Error{code: "internal_error", detail: {:tx_aborted, msg}}} =
             Outcome.classify_tx(%{"result" => results}, @verdicts, @tx6)

    assert msg =~ "backlog_underflow"
  end

  test "all-placeholder shortened reply is a definite cancel — rejected" do
    # Cancel / tx-creation-failure path: every in-tx entry is a
    # placeholder, COMMIT omitted — the transaction provably never
    # committed.
    results = [ok(nil), cancelled(), cancelled(), cancelled(), ok(decision("commit"))]

    assert {:rejected, %Error{code: "internal_error", detail: {:tx_aborted, _}}} =
             Outcome.classify_tx(%{"result" => results}, @verdicts, @tx6)
  end

  test "commit-failure rewrite shape is inconclusive — COMMIT omitted, all Internal" do
    # txn.commit() error: every in-tx entry rewritten to Internal (=
    # MULTIPLE real errors), COMMIT entry omitted, RETURN shifted to
    # index 4. The storage commit may already be durable — never a
    # definite rejection, and never read the shifted RETURN as a commit.
    for shifted <- [ok(decision("commit")), rewrite("decision blew up")] do
      results = [ok(nil), rewrite(), rewrite(), rewrite(), shifted]

      assert {:inconclusive, {:commit_result_unknown, msg}} =
               Outcome.classify_tx(%{"result" => results}, @verdicts, @tx6),
             "shifted #{inspect(shifted)}"

      assert msg =~ "grouped WAL sync failed"
    end
  end

  test "single in-tx statement shortened reply is ambiguous — inconclusive" do
    # ci = 2: ONE in-tx slot. One real error there is indistinguishable
    # between a lone abort and the commit-failure rewrite without
    # message substrings → inconclusive.
    ctx = %{commit: 2, decision: 3, len: 4}
    results = [ok(nil), rewrite(), ok(decision("commit"))]

    assert {:inconclusive, :shortened_shape_unqualified} =
             Outcome.classify_tx(%{"result" => results}, @verdicts, ctx)
  end

  test "unsupported shortened silhouettes stay inconclusive" do
    for results <- [
          # OK entry among in-tx — no failure path produces that
          [ok(nil), ok(nil), rewrite(), cancelled(), ok(decision("commit"))],
          # real error + a non-marker OK sibling
          [ok(nil), rewrite(), ok(%{}), cancelled(), ok(decision("commit"))],
          # non-OK BEGIN
          [rewrite(), not_executed(), rewrite(), cancelled(), ok(decision("commit"))],
          # reversed marker order: Cancelled before the cause,
          # NotExecuted after — not the executor's abort order
          [
            ok(nil),
            cancelled(),
            real_err("Internal", "x"),
            not_executed(),
            ok(decision("commit"))
          ],
          # nested lookalike: marker shape buried inside `result`
          # (the pre-capture wrong nesting assumption)
          [
            ok(nil),
            not_executed(),
            %{"status" => "ERR", "result" => %{"details" => %{"kind" => "Thrown"}}},
            cancelled(),
            ok(decision("commit"))
          ],
          # ERR cause without a message-string result
          [
            ok(nil),
            not_executed(),
            %{"status" => "ERR", "kind" => "Thrown", "result" => nil},
            cancelled(),
            ok(decision("commit"))
          ],
          # mixed all-marker set (NotExecuted + Cancelled, no real
          # cause) — the two producer paths never mix markers
          [ok(nil), cancelled(), not_executed(), cancelled(), ok(decision("commit"))]
        ] do
      assert {:inconclusive, _} =
               Outcome.classify_tx(%{"result" => results}, @verdicts, @tx6),
             "results #{inspect(results)}"
    end
  end

  test "a real error AT the commit slot in a full-length reply is unqualified" do
    reply = %{
      "result" => [
        ok(nil),
        real_err("Internal", "kv conflict"),
        ok(decision("commit"))
      ]
    }

    assert {:inconclusive, {:commit_result_unknown, msg}} =
             Outcome.classify_tx(reply, @verdicts, @ctx)

    assert msg =~ "kv conflict"
  end

  test "a contradictory pre-commit error in a full-length reply is inconclusive" do
    # Full-length + ERR among pre-COMMIT entries: a real in-tx error
    # would have aborted and shortened the reply — contradiction.
    results = [
      ok(nil),
      ok(nil),
      real_err("Internal", "impossible here"),
      ok(nil),
      ok(nil),
      ok(decision("commit"))
    ]

    assert {:inconclusive, :contradictory_precommit_errors} =
             Outcome.classify_tx(%{"result" => results}, @verdicts, @tx6)
  end

  test "a non-nil COMMIT value or marker at the commit slot cannot establish success" do
    for commit_entry <- [
          # marker-shaped ERR at the COMMIT slot (executor never emits this)
          not_executed(),
          # OK but non-nil — COMMIT returns NONE; a value here is unqualified
          ok(%{"some" => "value"}),
          # real error
          rewrite("x")
        ] do
      results = [ok(nil), ok(nil), ok(nil), ok(nil), commit_entry, ok(decision("commit"))]

      assert {:inconclusive, _} =
               Outcome.classify_tx(%{"result" => results}, @verdicts, @tx6),
             "commit entry #{inspect(commit_entry)}"
    end
  end

  test "a post-commit error cannot un-commit an observed-OK COMMIT" do
    # COMMIT OK, RETURN slot ERR — the commit stands; no decision →
    # inconclusive, never rejected.
    reply = %{
      "result" => [
        ok(nil),
        ok(nil),
        real_err("Internal", "array::at on NONE")
      ]
    }

    assert {:inconclusive, :committed_no_decision} = Outcome.classify_tx(reply, @verdicts, @ctx)
  end

  test "replay verdict maps to committed (frozen port); stale/conflict stay distinct rejections" do
    # Frozen contract: identical-transition replay is {:committed, _};
    # `existing` is accept-only. Conflicting replay content is an
    # identity collision → operation_id_conflict; a moved-on record is
    # stale_attempt — the codes are not interchangeable.
    assert {:committed, _} =
             Outcome.classify_tx(
               %{"result" => [ok(nil), ok(nil), ok(decision("replay"))]},
               @verdicts,
               @ctx
             )

    assert {:rejected, %Error{code: "stale_attempt"}} =
             Outcome.classify_tx(
               %{"result" => [ok(nil), ok(nil), ok(decision("stale"))]},
               @verdicts,
               @ctx
             )

    assert {:rejected, %Error{code: "operation_id_conflict"}} =
             Outcome.classify_tx(
               %{"result" => [ok(nil), ok(nil), ok(decision("conflict"))]},
               @verdicts,
               @ctx
             )
  end

  test "unmapped decision strings are inconclusive, not committed" do
    reply = %{"result" => [ok(nil), ok(nil), ok(decision("surprise"))]}

    assert {:inconclusive, {:unmapped_decision, "surprise"}} =
             Outcome.classify_tx(reply, @verdicts, @ctx)
  end

  test "strict fallback (no ctx): any ERR anywhere is inconclusive" do
    reply = %{"result" => [real_err("Internal", "x"), ok(decision("commit"))]}
    assert {:inconclusive, :statement_errors} = Outcome.classify_tx(reply, @verdicts)

    clean = %{"result" => [ok(decision("commit"))]}
    assert {:committed, _} = Outcome.classify_tx(clean, @verdicts)
  end

  test "reply lengths that match neither full nor shortened are inconclusive" do
    # len 3 ctx: 2 entries is shortened-but-ci<2 (unqualified), 1 entry
    # and 5 entries match no produced shape at all.
    assert {:inconclusive, :shortened_shape_unqualified} =
             Outcome.classify_tx(
               %{"result" => [ok(nil), cancelled()]},
               @verdicts,
               @ctx
             )

    assert {:inconclusive, {:unexpected_result_count, 1, 3}} =
             Outcome.classify_tx(%{"result" => [ok(nil)]}, @verdicts, @ctx)

    assert {:inconclusive, {:unexpected_result_count, 5, 3}} =
             Outcome.classify_tx(
               %{"result" => [ok(nil), ok(nil), ok(nil), ok(nil), ok(nil)]},
               @verdicts,
               @ctx
             )
  end

  test "non-list result and non-map replies are inconclusive" do
    assert {:inconclusive, :bad_reply_shape} =
             Outcome.classify_tx(%{"result" => "x"}, @verdicts, @ctx)

    assert {:inconclusive, :bad_reply_shape} = Outcome.classify_tx(42, @verdicts, @ctx)
  end
end
