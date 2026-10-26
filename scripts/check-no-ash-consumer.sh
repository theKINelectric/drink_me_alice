#!/usr/bin/env bash
# Compile AND exercise a scratch consumer that depends on this library
# WITHOUT Ash.
#
# The suite always runs with Ash installed, so a green suite alone does
# not prove the optional dependency is truly optional (A-08): the Ash
# data layer must compile only when the consumer brings Ash, and the
# rest of the library must work without it. The demo uses PUBLIC
# operations only — a valid transaction build, a protected-parameter
# refusal, and a typed session refusal against a closed port — no
# fabricated structs (review follow-up).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d /tmp/dma_consumer.XXXXXX)"
trap 'rm -rf "$work"' EXIT

cd "$work"
mix new consumer --sup >/dev/null

cat > consumer/mix.exs <<MIX
defmodule Consumer.MixProject do
  use Mix.Project

  def project do
    [
      app: :consumer,
      version: "0.1.0",
      elixir: "~> 1.18",
      deps: deps()
    ]
  end

  def application, do: [extra_applications: [:logger]]

  defp deps do
    [{:drink_me_alice, path: "${root}"}]
  end
end
MIX

cat > consumer/lib/consumer.ex <<'ELIXIR'
defmodule Consumer do
  @doc """
  Public-API smoke check without Ash and without a server: build a
  valid transaction, refuse a protected parameter, and expect a typed
  (never raising) session refusal against a closed port.
  """
  def demo do
    {:ok, _tx} =
      DrinkMeAlice.Tx.build(
        [
          "BEGIN TRANSACTION",
          "LET $x = 1",
          "LET $y = 2",
          "COMMIT TRANSACTION",
          "RETURN {decision: \"x\"}"
        ],
        %{}
      )

    {:error, {:protected_param, "auth"}} = DrinkMeAlice.check_param_names(%{"auth" => 1})

    case DrinkMeAlice.Session.open(%{
           host: "127.0.0.1",
           port: 1,
           ns: nil,
           db: nil,
           user: "u",
           pass: "p",
           connect_timeout: 250
         }) do
      {:error, _typed_refusal} -> :ok
      other -> raise "expected a typed refusal from a closed port, got: #{inspect(other)}"
    end
  end
end
ELIXIR

cd consumer
mix deps.get >/dev/null
mix compile --warnings-as-errors

# Ash must NOT be in the dependency tree at all.
if mix deps | grep -q "ash"; then
  echo "FAIL: ash leaked into the no-Ash consumer's dependency tree" >&2
  exit 1
fi

mix run -e "Consumer.demo()"
echo "no-Ash consumer: OK"
