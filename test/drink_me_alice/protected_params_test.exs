defmodule DrinkMeAlice.ProtectedParamsTest do
  @moduledoc """
  `$session`, `$auth`, `$token` and `$access` are protected by SurrealDB
  3.0.5: binding one makes the server refuse the whole request (-32600,
  measured against the pinned server). The client refuses them first.
  """
  use ExUnit.Case, async: true

  test "protected names are refused, as strings or atoms; others pass" do
    for name <- ~w(session auth token access) do
      assert {:error, {:protected_param, ^name}} = DrinkMeAlice.check_param_names(%{name => 1})

      assert {:error, {:protected_param, ^name}} =
               DrinkMeAlice.check_param_names(%{String.to_atom(name) => 1})
    end

    assert :ok = DrinkMeAlice.check_param_names(%{"sid" => 1, value: 2, before: 3})
  end

  test "transactions and queries refuse them before anything is sent" do
    stmts = [
      "BEGIN TRANSACTION",
      "LET $a = 1",
      "LET $b = 2",
      "COMMIT TRANSACTION",
      "RETURN {decision: \"ok\"}"
    ]

    assert {:error, {:protected_param, "session"}} =
             DrinkMeAlice.Tx.build(stmts, %{session: "s1"})

    # No connection is needed: the guard runs first.
    assert {:error, {:protected_param, "token"}} =
             DrinkMeAlice.query(:no_such_conn, "RETURN $token;", %{"token" => "x"})
  end
end
