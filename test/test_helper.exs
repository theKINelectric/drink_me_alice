# `:live` tests start a private loopback SurrealDB child per module.
# They are qualified against SurrealDB 3.0.5 ONLY: with a different (or
# missing) installed server they are skipped with the reason printed
# below — set SURREAL_ALLOW_UNQUALIFIED=1 to run them anyway as a
# compatibility experiment whose results are not a qualification.
#
# SURREAL_REQUIRE_QUALIFIED=1 (CI): anything that would be skipped
# instead FAILS the run here — a qualification job must never go green
# by quietly excluding its live suite.
# `mix test --exclude live` runs the transport/unit tests only.
case DrinkMeAlice.TestServer.live_policy() do
  :ok ->
    ExUnit.start(max_cases: 1, timeout: 120_000)

  {:skip, reason} ->
    IO.puts("[drink_me_alice] live tests skipped: #{reason}")
    ExUnit.start(max_cases: 1, timeout: 120_000, exclude: [:live])

  {:error, reason} ->
    IO.puts(:stderr, "[drink_me_alice] required live qualification cannot run: #{reason}")
    raise "SURREAL_REQUIRE_QUALIFIED=1 but the live suite cannot run: #{reason}"
end
