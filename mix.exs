defmodule DrinkMeAlice.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :drink_me_alice,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "A bounded SurrealDB client for Elixir (qualified against SurrealDB 3.0.5): " <>
          "supervised WebSocket connection, honest transaction outcomes, migrations, " <>
          "graph edges and an Ash data layer.",
      package: package(),
      docs: [main: "readme", extras: ["README.md", "CHANGELOG.md"]]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  # `:inets` is optional: on the code path, started on demand by
  # DrinkMeAlice.TestServer (its /health probe uses `:httpc`).
  def application do
    [extra_applications: [:logger, :crypto, inets: :optional]]
  end

  defp deps do
    [
      {:mint, "~> 1.10"},
      {:mint_web_socket, "~> 1.0"},
      # `DrinkMeAlice.Ash.DataLayer` compiles only when the consumer brings Ash.
      {:ash, "~> 3.33", optional: true},
      {:ex_doc, "~> 0.36", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      files: ~w(lib mix.exs README.md CHANGELOG.md PROVENANCE.md LICENSE),
      links: %{"GitHub" => "https://github.com/theKINelectric/drink_me_alice"}
    ]
  end
end
