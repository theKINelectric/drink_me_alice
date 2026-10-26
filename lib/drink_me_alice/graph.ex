defmodule DrinkMeAlice.Graph do
  @moduledoc """
  Graph edges between records — SurrealDB's `RELATE` — with the same
  honest write outcomes as `DrinkMeAlice.Tx`.

  Edge tables are `TYPE RELATION` tables (DDL via
  `DrinkMeAlice.Ash.Schema.edge_statements/4`, or by hand). A node is
  `{table, key}` where `key` is a string or integer record key.

      {:committed, _} = Graph.relate(conn, "blocks", {"task", "a"}, {"task", "b"}, %{"why" => "needs api"})
      {:ok, ["b"]}    = Graph.outgoing(conn, {"task", "a"}, "blocks", "task")

  Table and edge names must be identifiers (`DrinkMeAlice.identifier?/1`);
  keys and field values travel as bound params. String keys and
  top-level string field values travel exact (`DrinkMeAlice.exact_string/1` —
  3.0.5 coerces date-, UUID- and record-shaped strings); nested field
  values are canonical terms and are NOT protected from that coercion.
  Field names must be identifiers.
  """

  alias DrinkMeAlice.Tx

  @max_fields 32
  @max_neighbors 1_000

  @type node_ref :: {binary(), binary() | integer()}

  @doc """
  Create one edge `from -[edge]-> to`. Returns a `Tx.run/4` outcome;
  `{:committed, %{"edge" => edge_row}}` on success.
  """
  @spec relate(GenServer.server(), binary(), node_ref(), node_ref(), map(), keyword()) ::
          Tx.outcome() | {:error, term()}
  def relate(conn, edge, from, to, fields \\ %{}, opts \\ []) do
    with :ok <- idents([edge]),
         {:ok, from_sql, from_vars} <- node(from, "a"),
         {:ok, to_sql, to_vars} <- node(to, "b"),
         {:ok, set_sql, set_vars} <- fields(fields) do
      statements = [
        "BEGIN TRANSACTION",
        "LET $__from = #{from_sql}",
        "LET $__to = #{to_sql}",
        "LET $__edge = (RELATE ONLY $__from->#{edge}->$__to#{set_sql} RETURN AFTER)",
        "COMMIT TRANSACTION",
        "RETURN {decision: \"related\", edge: $__edge}"
      ]

      with {:ok, tx} <- Tx.build(statements, Map.merge(from_vars, to_vars) |> Map.merge(set_vars)) do
        Tx.run(conn, tx, %{"related" => :committed}, opts)
      end
    end
  end

  @doc "Delete every `from -[edge]-> to` edge. A `Tx.run/4` outcome."
  @spec unrelate(GenServer.server(), binary(), node_ref(), node_ref(), keyword()) ::
          Tx.outcome() | {:error, term()}
  def unrelate(conn, edge, from, to, opts \\ []) do
    with :ok <- idents([edge]),
         {:ok, from_sql, from_vars} <- node(from, "a"),
         {:ok, to_sql, to_vars} <- node(to, "b") do
      statements = [
        "BEGIN TRANSACTION",
        "LET $__from = #{from_sql}",
        "LET $__to = #{to_sql}",
        "DELETE #{edge} WHERE in = $__from AND out = $__to",
        "COMMIT TRANSACTION",
        "RETURN {decision: \"unrelated\"}"
      ]

      with {:ok, tx} <- Tx.build(statements, Map.merge(from_vars, to_vars)) do
        Tx.run(conn, tx, %{"unrelated" => :committed}, opts)
      end
    end
  end

  @doc """
  Keys of the `to_table` records reachable by one outgoing `edge` hop,
  bounded at #{@max_neighbors} (more is `{:error, {:bounded_limit_exceeded, :neighbors}}`).
  """
  @spec outgoing(GenServer.server(), node_ref(), binary(), binary(), keyword()) ::
          {:ok, list()} | {:error, term()}
  def outgoing(conn, from, edge, to_table, opts \\ []),
    do: hop(conn, from, "->#{edge}->#{to_table}", [edge, to_table], opts)

  @doc "Keys of the `from_table` records with an `edge` INTO `to` (one incoming hop), bounded likewise."
  @spec incoming(GenServer.server(), node_ref(), binary(), binary(), keyword()) ::
          {:ok, list()} | {:error, term()}
  def incoming(conn, to, edge, from_table, opts \\ []),
    do: hop(conn, to, "<-#{edge}<-#{from_table}", [edge, from_table], opts)

  defp hop(conn, start, path, names, opts) do
    with :ok <- idents(names),
         {:ok, start_sql, vars} <- node(start, "a") do
      sql =
        "SELECT VALUE array::slice(#{path}.map(|$r| record::id($r)), 0, #{@max_neighbors + 1}) " <>
          "FROM ONLY #{start_sql};"

      case DrinkMeAlice.query(conn, sql, vars, opts) do
        {:ok, [%{"status" => "OK", "result" => keys}]} when is_list(keys) ->
          if length(keys) > @max_neighbors,
            do: {:error, {:bounded_limit_exceeded, :neighbors}},
            else: {:ok, keys}

        {:ok, [%{"status" => "OK", "result" => nil}]} ->
          {:ok, []}

        {:ok, [%{"status" => "ERR", "result" => msg}]} ->
          {:error, {:statement_err, String.slice(to_string(msg), 0, 160)}}

        {:ok, _other} ->
          {:error, :bad_reply}

        {:error, e} ->
          {:error, e}
      end
    end
  end

  defp idents(names) do
    case Enum.reject(names, &DrinkMeAlice.identifier?/1) do
      [] -> :ok
      bad -> {:error, {:not_identifier, bad}}
    end
  end

  defp node({table, key}, p) when is_binary(key) do
    with :ok <- idents([table]),
         do:
           {:ok, "type::record(<string>$#{p}_tb, #{DrinkMeAlice.exact_string_sql("#{p}_k")})",
            %{"#{p}_tb" => table, "#{p}_k" => DrinkMeAlice.exact_string(key)}}
  end

  defp node({table, key}, p) when is_integer(key) do
    with :ok <- idents([table]),
         do:
           {:ok, "type::record(<string>$#{p}_tb, $#{p}_k)",
            %{"#{p}_tb" => table, "#{p}_k" => key}}
  end

  defp node(other, _p), do: {:error, {:bad_node, other}}

  defp fields(fields) when map_size(fields) == 0, do: {:ok, "", %{}}

  defp fields(fields) when is_map(fields) and map_size(fields) <= @max_fields do
    fields
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, [], %{}}, fn {{k, v}, i}, {:ok, parts, vars} ->
      name = to_string(k)

      if DrinkMeAlice.identifier?(name) and name not in ["id", "in", "out"] do
        # Top-level strings travel exact (see `DrinkMeAlice.exact_string/1`);
        # nested values are canonical terms subject to server coercion.
        {ref, wire} =
          if is_binary(v),
            do: {DrinkMeAlice.exact_string_sql("e#{i}"), DrinkMeAlice.exact_string(v)},
            else: {"$e#{i}", v}

        {:cont, {:ok, ["#{name} = #{ref}" | parts], Map.put(vars, "e#{i}", wire)}}
      else
        {:halt, {:error, {:bad_field, name}}}
      end
    end)
    |> case do
      {:ok, parts, vars} -> {:ok, " SET " <> Enum.join(Enum.reverse(parts), ", "), vars}
      e -> e
    end
  end

  defp fields(_), do: {:error, {:bounded_limit_exceeded, :edge_fields}}
end
