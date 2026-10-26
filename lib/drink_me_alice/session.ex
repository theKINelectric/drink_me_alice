defmodule DrinkMeAlice.Session do
  @moduledoc """
  A caller-owned, synchronous SurrealDB session: one socket, owned by the
  process that opened it, one request at a time, no background process.

  For short-lived work — scripts, provisioning, migrations, drills —
  where a supervised `DrinkMeAlice.Conn` owner is more machinery than
  the job needs. Same transport (`Ws`), same frame guard and same bounded
  JSON decode profile as `Conn`; no credits (the caller is the only
  sender) and no reconnect (a lost transport is reported, and the caller
  opens a fresh session).

  The session value carries the Mint/WebSocket parser state; every call
  returns the updated session, so fragmented replies never leave stale
  buffer state for the next call. Close it with `close/1` — `Ws.close/1`
  is idempotent, and a dead session is safe to close.

  Credentials never enter error values: failures are sanitized codes.

  ## Config

  `host`, `port`, `ns`, `db`, `user`, `pass`, `signin` (`:root` |
  `:namespace` | `:database`, default `:root`), `connect_timeout`
  (default 2_000), `reply_budget_ms` (default 5_000, at most 60_000).
  A root session may set `ns: nil` — or omit `ns` entirely; the two are
  normalized to the same thing — and a root or namespace session may set
  `db: nil` or omit it.
  """

  alias DrinkMeAlice.{Conn, Json, Ws}

  @default_reply_budget_ms 5_000
  @max_reply_budget_ms 60_000

  @derive {Inspect, only: [:ns, :db]}
  defstruct [:ws, :ns, :db, reply_budget_ms: @default_reply_budget_ms]

  @opaque t :: %__MODULE__{}

  @doc """
  Connect, sign in, `use ns db`. Closes the socket on any failed step —
  including an unexpected exception — and never raises or exits: an
  omitted optional key and an explicit `nil` are the same
  configuration, and even a malformed top-level configuration form is
  a typed `{:error, _}`, never an exception.
  """
  @spec open(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def open(config) do
    # The WHOLE setup boundary is covered — normalization, validation,
    # connect and establishment: `Map.new/1` of a malformed list and
    # `Ws.connect/4` of an unusable endpoint are typed errors here too,
    # not exceptions (review R-06).
    try do
      # Optional scope keys become explicit `nil`s before anything reads
      # this map: `ns`/`db` omitted and `ns: nil`/`db: nil` must behave
      # identically, and no later step may raise a key-access exception
      # whose message would print this credential-bearing map (A-01).
      c =
        config
        |> Map.new()
        |> Map.put_new(:ns, nil)
        |> Map.put_new(:db, nil)

      with {:ok, budget} <- reply_budget(c),
           :ok <- check(c) do
        case Ws.connect(c.host, c.port, "/rpc",
               connect_timeout: Map.get(c, :connect_timeout, 2_000)
             ) do
          {:ok, ws, early} ->
            # The socket is acquired from here on: every step runs under
            # a rescue/catch so no possible exception can leak it — the
            # typed error carries a stable tag, never the config map.
            try do
              with :ok <- absorb_early(ws, early),
                   {:ok, ws} <- rpc_or_close(ws, "signin", [signin_params(c)], budget),
                   {:ok, ws} <- use_scope(ws, c, budget) do
                {:ok, %__MODULE__{ws: ws, ns: c.ns, db: c.db, reply_budget_ms: budget}}
              end
            rescue
              e ->
                _ = Ws.close(ws)
                {:error, {:open_exception, crash_tag(e)}}
            catch
              kind, _value ->
                _ = Ws.close(ws)
                {:error, {:open_exception, {:caught, kind}}}
            end

          {:error, r} ->
            {:error, r}
        end
      end
    rescue
      e ->
        {:error, {:open_exception, crash_tag(e)}}
    catch
      kind, _value ->
        {:error, {:open_exception, {:caught, kind}}}
    end
  end

  # Stable, secret-free classification of an unexpected setup crash.
  defp crash_tag(%KeyError{}), do: :key_error
  defp crash_tag(%Protocol.UndefinedError{}), do: :protocol_error
  defp crash_tag(%ArgumentError{}), do: :argument_error
  defp crash_tag(_other), do: :unexpected

  # Root sessions may leave `ns` (and namespace sessions `db`) unset —
  # for DEFINE NAMESPACE / DEFINE DATABASE and other scope-level work.
  defp signin_params(c) do
    c = Map.put_new(c, :signin, :root)

    case c.signin do
      :root -> %{"user" => c.user, "pass" => c.pass}
      _ -> Conn.signin_params(c)
    end
  end

  defp use_scope(ws, %{ns: nil}, _budget), do: {:ok, ws}
  defp use_scope(ws, c, budget), do: rpc_or_close(ws, "use", [c.ns, Map.get(c, :db)], budget)

  defp check(c) do
    signin = Map.get(c, :signin, :root)

    cond do
      not (is_binary(c[:host]) and is_integer(c[:port]) and c[:port] in 1..65_535) ->
        {:error, {:invalid_config, :endpoint}}

      not valid_connect_timeout?(c[:connect_timeout]) ->
        {:error, {:invalid_config, :connect_timeout}}

      not (DrinkMeAlice.identifier?(c[:ns]) or (c[:ns] == nil and signin == :root)) ->
        {:error, {:invalid_config, :ns}}

      not (DrinkMeAlice.identifier?(c[:db]) or (c[:db] == nil and signin != :database)) ->
        {:error, {:invalid_config, :db}}

      c[:ns] == nil and c[:db] != nil ->
        {:error, {:invalid_config, :db}}

      not (is_binary(c[:user]) and is_binary(c[:pass])) ->
        {:error, {:invalid_config, :credentials}}

      Map.get(c, :signin, :root) not in [:root, :namespace, :database] ->
        {:error, {:invalid_config, :signin}}

      true ->
        :ok
    end
  end

  # Unset is the documented 2_000 default; an explicit value must be a
  # non-negative integer — anything else would reach the TCP layer and
  # exit there (review R-06).
  defp valid_connect_timeout?(nil), do: true

  defp valid_connect_timeout?(v) when is_integer(v) and v >= 0, do: true

  defp valid_connect_timeout?(_), do: false

  defp reply_budget(c) do
    case Map.get(c, :reply_budget_ms, @default_reply_budget_ms) do
      v when is_integer(v) and v in 1..@max_reply_budget_ms -> {:ok, v}
      other -> {:error, {:invalid_option, :reply_budget_ms, other}}
    end
  end

  @doc """
  Run one `query`. `{:ok, session, results}` where `results` is the
  per-statement array (callers check every status), or
  `{:error, reason}` — `{:rpc_error, error_map}` for a server refusal
  (the whole object, so `Outcome.session_refusal?/1` can read it),
  `{:lost, reason}` when the request may have been sent but no reply
  arrived. After `{:lost, _}` the session is not reusable: close it.
  """
  @spec query(t(), binary(), map()) :: {:ok, t(), list()} | {:error, term()}
  def query(%__MODULE__{} = s, sql, vars \\ %{}) when is_binary(sql) and is_map(vars) do
    with :ok <- DrinkMeAlice.check_param_names(vars), do: do_query(s, sql, vars)
  end

  defp do_query(s, sql, vars) do
    case request(s, "query", [sql, DrinkMeAlice.stringify_param_names(vars)]) do
      {:ok, s, %{"result" => results}} when is_list(results) -> {:ok, s, results}
      {:ok, _s, %{"error" => %{} = err}} -> {:error, {:rpc_error, err}}
      {:ok, _s, _other} -> {:error, :bad_reply}
      {:error, r} -> {:error, r}
    end
  end

  @doc "Run one RPC method; `{:ok, session, result}` or `{:error, reason}`."
  @spec rpc(t(), binary(), list()) :: {:ok, t(), term()} | {:error, term()}
  def rpc(%__MODULE__{} = s, method, params) when is_binary(method) and is_list(params) do
    case request(s, method, params) do
      {:ok, s, %{"result" => r}} -> {:ok, s, r}
      {:ok, _s, %{"error" => %{} = err}} -> {:error, {:rpc_error, err}}
      {:ok, _s, _other} -> {:error, :bad_reply}
      {:error, r} -> {:error, r}
    end
  end

  @doc "Close the socket. Idempotent."
  @spec close(t()) :: :ok
  def close(%__MODULE__{ws: ws}) do
    _ = Ws.close(ws)
    :ok
  end

  @doc """
  Open a session, run `fun.(query)` and close — the socket is closed on
  every exit path. `query` is `(sql, vars) -> {:ok, results} | {:error,
  reason}`. Returns `{:ok, fun_result}` or `{:error, reason}` when the
  session could not open; a body that returns `{:error, r}` surfaces as
  `{:error, r}`, never as `{:ok, {:error, r}}`.
  """
  @spec with_session(map() | keyword(), (function() -> term())) ::
          :ok | {:ok, term()} | {:error, term()}
  def with_session(config, fun) when is_function(fun, 1) do
    with {:ok, s} <- open(config),
         {:ok, agent} <- Agent.start_link(fn -> s end) do
      query = fn sql, vars ->
        case query(Agent.get(agent, & &1), sql, vars) do
          {:ok, s2, results} ->
            Agent.update(agent, fn _ -> s2 end)
            {:ok, results}

          {:error, r} ->
            {:error, r}
        end
      end

      try do
        unwrap(fun.(query))
      after
        close(Agent.get(agent, & &1))
        Agent.stop(agent)
      end
    end
  end

  defp unwrap(:ok), do: :ok
  defp unwrap({:error, r}), do: {:error, r}
  defp unwrap({:ok, v}), do: {:ok, v}
  defp unwrap(other), do: {:ok, other}

  # -- internals --------------------------------------------------------------

  defp request(%__MODULE__{ws: ws} = s, method, params) do
    id = :erlang.unique_integer([:positive, :monotonic])

    with {:ok, raw} <- encode(%{"id" => id, "method" => method, "params" => params}),
         {:ok, ws} <- send_frame(ws, raw),
         {:ok, ws, reply} <- await_reply(ws, id, s.reply_budget_ms) do
      {:ok, %{s | ws: ws}, reply}
    end
  end

  defp rpc_or_close(ws, method, params, budget) do
    id = :erlang.unique_integer([:positive, :monotonic])

    result =
      with {:ok, raw} <- encode(%{"id" => id, "method" => method, "params" => params}),
           {:ok, ws} <- send_frame(ws, raw),
           {:ok, ws, reply} <- await_reply(ws, id, budget) do
        case reply do
          %{"error" => %{"code" => code}} -> {:error, {:rpc_error, code}}
          %{"result" => _} -> {:ok, ws}
          _ -> {:error, :bad_reply}
        end
      end

    case result do
      {:ok, ws} ->
        {:ok, ws}

      {:error, r} ->
        _ = Ws.close(ws)
        {:error, r}
    end
  end

  # A failed send may have written a partial frame — the request may have
  # entered the write path, so it is `lost`, never `refused`.
  defp send_frame(ws, raw) do
    case Ws.send_frame(ws, {:text, raw}) do
      {:ok, ws} -> {:ok, ws}
      {:error, _ws, _reason} -> {:error, {:lost, :send_failed}}
    end
  end

  defp await_reply(ws, id, timeout_ms) do
    loop(ws, id, System.monotonic_time(:millisecond) + timeout_ms)
  end

  defp loop(ws, id, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:error, {:lost, :timeout}}
    else
      case Ws.recv_frames(ws, min(remaining, 250)) do
        {:ok, ws, frames} ->
          case find(frames, id) do
            {:found, reply} -> {:ok, ws, reply}
            :no -> loop(ws, id, deadline)
            {:fatal, r} -> {:error, {:lost, r}}
          end

        {:timeout, ws} ->
          loop(ws, id, deadline)

        {:closed, _ws, _frames} ->
          {:error, {:lost, :closed}}

        {:error, _ws, r} ->
          {:error, {:lost, r}}
      end
    end
  end

  defp find(frames, id) do
    Enum.reduce_while(frames, :no, fn
      {:text, raw}, _ ->
        case Json.decode(raw) do
          {:ok, %{"id" => ^id} = reply} -> {:halt, {:found, reply}}
          {:ok, _} -> {:cont, :no}
          {:error, _} -> {:halt, {:fatal, :malformed}}
        end

      {:malformed, _}, _ ->
        {:halt, {:fatal, :malformed}}

      {:close, _, _}, _ ->
        {:halt, {:fatal, :closed}}

      _, _ ->
        {:cont, :no}
    end)
  end

  defp absorb_early(ws, frames) do
    if Enum.any?(frames, &match?({:malformed, _}, &1)) do
      _ = Ws.close(ws)
      {:error, :early_malformed}
    else
      :ok
    end
  end

  defp encode(msg) do
    case Json.encode(msg) do
      {:ok, raw} when byte_size(raw) <= 1_048_576 -> {:ok, raw}
      {:ok, _} -> {:error, :too_large}
      {:error, e} -> {:error, {:encode, e.code}}
    end
  end
end
