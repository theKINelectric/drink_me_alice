defmodule DrinkMeAlice.Error do
  @moduledoc """
  A typed error: a stable `code` string and a bounded `detail` term.

  Codes are stable strings (`malformed_message`,
  `bounded_limit_exceeded`, `invalid_input`, `internal_error`,
  `database_unavailable`, …) so a host can map this struct onto its own
  error type with a field copy.
  """

  defstruct [:code, :detail]

  @type t :: %__MODULE__{code: binary(), detail: term()}

  @spec new(binary(), term()) :: t()
  def new(code, detail \\ nil), do: %__MODULE__{code: code, detail: detail}

  @spec malformed(term()) :: t()
  def malformed(detail), do: new("malformed_message", detail)

  @spec bounded_exceeded(term()) :: t()
  def bounded_exceeded(detail), do: new("bounded_limit_exceeded", detail)

  @spec invalid(term()) :: t()
  def invalid(detail), do: new("invalid_input", detail)
end
