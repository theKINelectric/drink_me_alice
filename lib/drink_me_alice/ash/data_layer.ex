if Code.ensure_loaded?(Ash.DataLayer) do
  defmodule DrinkMeAlice.Ash.DataLayer do
    @surreal %Spark.Dsl.Section{
      name: :surrealdb,
      describe: "Configure the SurrealDB data layer.",
      examples: [
        """
        surrealdb do
          conn MyApp.Surreal
          table "mission"
        end
        """
      ],
      schema: [
        conn: [
          type: :atom,
          required: true,
          doc: "The registered name of the `DrinkMeAlice.Conn` owner to use."
        ],
        table: [
          type: :string,
          doc: "The table name. Defaults to the resource's short name."
        ],
        max_rows: [
          type: :pos_integer,
          default: 1_000,
          doc:
            "Read bound (at most 10_000). A read that would return more rows is refused — never silently truncated."
        ],
        deadline_ms: [
          type: :pos_integer,
          default: 2_000,
          doc: "Per-request deadline (at most 2_000)."
        ]
      ]
    }

    @moduledoc """
    An Ash data layer over a supervised `DrinkMeAlice.Conn`.

        use Ash.Resource,
          domain: MyApp.Crew,
          data_layer: DrinkMeAlice.Ash.DataLayer

        surrealdb do
          conn MyApp.Surreal
          table "mission"
        end

    ## Storage

    The single primary-key attribute (string, UUID or integer) is the
    record id (`mission:⟨pk⟩`); every other attribute is a field.
    Schema DDL comes from `DrinkMeAlice.Ash.Schema`. Types map per
    `DrinkMeAlice.Ash.Codec`.

    ## Writes

    Each create/update/destroy is one `DrinkMeAlice.Tx` transaction, so
    the outcome is honest:

    - a duplicate primary key or a violated identity index →
      `Ash.Error.Changes.InvalidChanges` ("has already been taken");
    - an update/destroy whose record is gone or fails the changeset
      filter → `Ash.Error.Changes.StaleRecord` (checked inside the tx);
    - a lost or inconclusive reply → `DrinkMeAlice.Ash.OutcomeUnknown`
      — the write MAY have committed; reconcile by reading;
    - a certified non-send → `DrinkMeAlice.Ash.Unavailable`.

    ## Reads

    Filters are pushed down (see `DrinkMeAlice.Ash.Filter`) and sorts
    on plain attributes become `ORDER BY`; `limit`/`offset` become
    `LIMIT`/`START`. Every read is bounded by `max_rows`: a read that
    would exceed it is an error, never a truncated answer.
    Relationships load through `in` filters on the destination attribute.

    ## Not supported (yet)

    Transactions across actions, upserts, bulk/atomic/query updates,
    aggregates, calculations in the data layer, multitenancy, joins
    (`exists`, filters across relationships), and graph edges as Ash
    relationships (use `DrinkMeAlice.Graph` directly).
    """

    @behaviour Ash.DataLayer

    use Spark.Dsl.Extension, sections: [@surreal]

    alias Ash.Error.Changes.{InvalidChanges, StaleRecord}
    alias DrinkMeAlice.Error
    alias DrinkMeAlice.Ash.{Codec, Filter, OutcomeUnknown, QueryFailed, Unavailable}
    alias DrinkMeAlice.Tx

    @max_rows_cap 10_000

    defmodule Query do
      @moduledoc false
      defstruct [:resource, :domain, :filter, :limit, :tenant, sort: [], offset: 0, context: %{}]
    end

    # -- configuration --------------------------------------------------------

    @doc "The configured `Conn` name."
    def conn(resource), do: Spark.Dsl.Extension.get_opt(resource, [:surrealdb], :conn, nil, true)

    @doc "The table name (validated identifier)."
    def table(resource) do
      t =
        Spark.Dsl.Extension.get_opt(resource, [:surrealdb], :table, nil, true) ||
          resource |> Ash.Resource.Info.short_name() |> Atom.to_string()

      if DrinkMeAlice.identifier?(t),
        do: t,
        else: raise(ArgumentError, "table is not an identifier: #{inspect(t)}")
    end

    defp max_rows(resource),
      do:
        min(
          Spark.Dsl.Extension.get_opt(resource, [:surrealdb], :max_rows, 1_000, true),
          @max_rows_cap
        )

    defp deadline_ms(resource),
      do:
        min(Spark.Dsl.Extension.get_opt(resource, [:surrealdb], :deadline_ms, 2_000, true), 2_000)

    @doc "The single primary-key attribute name; raises unless exactly one."
    def primary_key!(resource) do
      case Ash.Resource.Info.primary_key(resource) do
        [pk] ->
          pk

        other ->
          raise ArgumentError,
                "#{inspect(resource)}: exactly one primary key required, got #{inspect(other)}"
      end
    end

    # -- capabilities -----------------------------------------------------------

    @impl true
    def can?(_, :read), do: true
    def can?(_, :create), do: true
    def can?(resource, :update), do: pk?(resource)
    def can?(resource, :destroy), do: pk?(resource)
    def can?(_, :filter), do: true
    def can?(_, :boolean_filter), do: true
    def can?(_, :sort), do: true
    def can?(_, :limit), do: true
    def can?(_, :offset), do: true
    def can?(_, :changeset_filter), do: true
    def can?(_, :nested_expressions), do: true
    def can?(resource, {:filter_expr, expr}), do: Filter.supported?(resource, expr)
    def can?(_, {:sort, storage}), do: sortable_storage?(storage)
    def can?(_, _), do: false

    # Scalars sort natively; JSON-text maps and arrays do not sort meaningfully.
    defp sortable_storage?(storage) do
      case Codec.wire_kind(storage) do
        {:ok, :str} -> true
        {:ok, :plain} -> true
        {:ok, {:cast, _}} -> true
        _ -> false
      end
    end

    defp pk?(resource), do: match?([_], Ash.Resource.Info.primary_key(resource))

    @impl true
    def source(resource), do: table(resource)

    # -- query building -----------------------------------------------------------

    @impl true
    def resource_to_query(resource, domain), do: %Query{resource: resource, domain: domain}

    @impl true
    def filter(query, filter, _resource) do
      if query.filter,
        do: {:ok, %{query | filter: Ash.Filter.add_to_filter!(query.filter, filter)}},
        else: {:ok, %{query | filter: filter}}
    end

    @impl true
    def sort(query, sort, _resource), do: {:ok, %{query | sort: sort}}

    @impl true
    def limit(query, limit, _resource), do: {:ok, %{query | limit: limit}}

    @impl true
    def offset(query, offset, _resource), do: {:ok, %{query | offset: offset || 0}}

    @impl true
    def select(query, _select, _resource), do: {:ok, query}

    @impl true
    def set_context(_resource, query, context), do: {:ok, %{query | context: context}}

    @impl true
    def set_tenant(_resource, query, tenant), do: {:ok, %{query | tenant: tenant}}

    # -- reads ------------------------------------------------------------------------

    @impl true
    def run_query(%Query{resource: resource} = query, _resource) do
      pk = primary_key!(resource)
      cap = max_rows(resource)

      with {:ok, take, strict?} <- take(query.limit, cap, resource),
           {:ok, {where, params}} <- where(Filter.to_where(query.filter, pk), resource),
           {:ok, order} <- order_by(query.sort, resource, pk) do
        sql =
          "SELECT *, record::id(id) AS __pk FROM #{table(resource)}" <>
            if(where == "", do: "", else: " WHERE " <> where) <>
            order <> " LIMIT #{take} START #{max(query.offset || 0, 0)};"

        with {:ok, rows} <- read(resource, sql, params) do
          if strict? and length(rows) > cap do
            {:error,
             QueryFailed.exception(
               resource: resource,
               reason: {:bounded_limit_exceeded, :max_rows, cap}
             )}
          else
            decode_rows(resource, pk, rows)
          end
        end
      end
    end

    # No limit → fetch cap+1 and refuse if the extra row appears. An
    # explicit limit above the cap is refused up front.
    defp take(nil, cap, _resource), do: {:ok, cap + 1, true}

    defp take(limit, cap, _resource) when is_integer(limit) and limit >= 0 and limit <= cap,
      do: {:ok, limit, false}

    defp take(limit, cap, resource),
      do:
        {:error,
         QueryFailed.exception(
           resource: resource,
           reason: {:bounded_limit_exceeded, :limit, limit, cap}
         )}

    defp where({:ok, w}, _resource), do: {:ok, w}

    defp where({:error, e}, resource),
      do: {:error, QueryFailed.exception(resource: resource, reason: e)}

    defp order_by([], _resource, _pk), do: {:ok, ""}

    defp order_by(sort, resource, pk) do
      Enum.reduce_while(sort, {:ok, []}, fn
        {field, dir}, {:ok, acc} when is_atom(field) ->
          with {:ok, d} <- direction(dir),
               {:ok, col} <- sort_column(resource, field, pk) do
            {:cont, {:ok, ["#{col} #{d}" | acc]}}
          else
            _ ->
              {:halt,
               {:error,
                QueryFailed.exception(resource: resource, reason: {:unsupported_sort, field, dir})}}
          end

        other, _acc ->
          {:halt,
           {:error, QueryFailed.exception(resource: resource, reason: {:unsupported_sort, other})}}
      end)
      |> case do
        {:ok, parts} -> {:ok, " ORDER BY " <> Enum.join(Enum.reverse(parts), ", ")}
        e -> e
      end
    end

    # SurrealDB orders NONE first ascending and last descending — the
    # defaults of :asc_nils_first / :desc_nils_last. Other nil placements
    # are refused rather than silently reordered.
    defp direction(d) when d in [:asc, :asc_nils_first], do: {:ok, "ASC"}
    defp direction(d) when d in [:desc, :desc_nils_last], do: {:ok, "DESC"}
    defp direction(_), do: :error

    defp sort_column(_resource, pk, pk), do: {:ok, "id"}

    defp sort_column(resource, field, _pk) do
      case Ash.Resource.Info.attribute(resource, field) do
        nil ->
          :error

        attr ->
          name = Atom.to_string(attr.source || attr.name)
          if DrinkMeAlice.identifier?(name), do: {:ok, name}, else: :error
      end
    end

    defp read(resource, sql, params) do
      case DrinkMeAlice.query(conn(resource), sql, params, deadline_ms: deadline_ms(resource)) do
        {:ok, [%{"status" => "OK", "result" => rows}]} when is_list(rows) ->
          {:ok, rows}

        {:ok, [%{"status" => "ERR", "result" => msg}]} ->
          {:error,
           QueryFailed.exception(resource: resource, reason: {:statement_err, short(msg)})}

        {:ok, other} ->
          {:error, QueryFailed.exception(resource: resource, reason: {:bad_reply, length(other)})}

        {:error, {:refused, reason}} ->
          {:error, Unavailable.exception(resource: resource, reason: reason)}

        {:error, :session_refused} ->
          {:error, Unavailable.exception(resource: resource, reason: :session_refused)}

        {:error, reason} ->
          {:error, QueryFailed.exception(resource: resource, reason: reason)}
      end
    end

    defp decode_rows(resource, pk, rows) do
      Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, acc} ->
        case decode_row(resource, pk, row) do
          {:ok, rec} -> {:cont, {:ok, [rec | acc]}}
          {:error, e} -> {:halt, {:error, e}}
        end
      end)
      |> case do
        {:ok, acc} -> {:ok, Enum.reverse(acc)}
        e -> e
      end
    end

    defp decode_row(resource, pk, row) do
      resource
      |> Ash.Resource.Info.attributes()
      |> Enum.reduce_while({:ok, %{}}, fn attr, {:ok, acc} ->
        raw =
          if attr.name == pk,
            do: Map.get(row, "__pk") || Map.get(row, "pk"),
            else: Map.get(row, Atom.to_string(attr.source || attr.name))

        case Codec.decode(attr, raw) do
          {:ok, v} -> {:cont, {:ok, Map.put(acc, attr.name, v)}}
          {:error, e} -> {:halt, {:error, QueryFailed.exception(resource: resource, reason: e)}}
        end
      end)
      |> case do
        {:ok, values} ->
          rec = struct(resource, values)
          {:ok, %{rec | __meta__: %Ecto.Schema.Metadata{state: :loaded, schema: resource}}}

        e ->
          e
      end
    end

    # -- writes ------------------------------------------------------------------------

    @impl true
    def create(resource, changeset) do
      pk = primary_key!(resource)

      with {:ok, record} <- Ash.Changeset.apply_attributes(changeset),
           {:ok, rid, rid_vars} <- record_id(resource, pk, Map.get(record, pk)),
           {:ok, sets, set_vars} <- sets(resource, pk, record, all_fields(resource, pk)) do
        statements = [
          "BEGIN TRANSACTION",
          "LET $__rid = #{rid}",
          "LET $__row = (CREATE ONLY $__rid SET #{sets} RETURN AFTER)",
          "COMMIT TRANSACTION",
          "RETURN {decision: \"created\", row: $__row, pk: record::id($__row.id)}"
        ]

        write(
          resource,
          :create,
          Map.get(record, pk),
          statements,
          Map.merge(rid_vars, set_vars),
          %{
            "created" => :committed
          }
        )
      end
    end

    @impl true
    def update(resource, changeset) do
      pk = primary_key!(resource)
      key = Map.get(changeset.data, pk)
      changed = changeset.attributes |> Map.keys() |> Enum.reject(&(&1 == pk))

      cond do
        Map.has_key?(changeset.attributes, pk) and Map.get(changeset.attributes, pk) != key ->
          {:error,
           QueryFailed.exception(resource: resource, reason: :primary_key_change_unsupported)}

        true ->
          with {:ok, record} <- Ash.Changeset.apply_attributes(changeset),
               {:ok, rid, rid_vars} <- record_id(resource, pk, key),
               {:ok, guard, guard_vars} <- guard(resource, pk, changeset.filter),
               {:ok, sets, set_vars} <- sets(resource, pk, record, changed) do
            write_row =
              if changed == [],
                do: "LET $__row = (SELECT * FROM ONLY $__rid)",
                else: "LET $__row = (UPDATE ONLY $__rid SET #{sets} RETURN AFTER)"

            statements = [
              "BEGIN TRANSACTION",
              "LET $__rid = #{rid}",
              "LET $__hit = (SELECT id FROM $__rid#{guard})",
              "IF array::len($__hit) = 0 { THROW \"bs:stale\" }",
              write_row,
              "COMMIT TRANSACTION",
              "RETURN {decision: \"updated\", row: $__row, pk: record::id($__row.id)}"
            ]

            vars = rid_vars |> Map.merge(guard_vars) |> Map.merge(set_vars)

            write(
              resource,
              :update,
              key,
              statements,
              vars,
              %{"updated" => :committed},
              changeset.filter
            )
          end
      end
    end

    @impl true
    def destroy(resource, changeset) do
      pk = primary_key!(resource)
      key = Map.get(changeset.data, pk)

      with {:ok, rid, rid_vars} <- record_id(resource, pk, key),
           {:ok, guard, guard_vars} <- guard(resource, pk, changeset.filter) do
        statements = [
          "BEGIN TRANSACTION",
          "LET $__rid = #{rid}",
          "LET $__hit = (SELECT id FROM $__rid#{guard})",
          "IF array::len($__hit) = 0 { THROW \"bs:stale\" }",
          "DELETE $__rid",
          "COMMIT TRANSACTION",
          "RETURN {decision: \"destroyed\"}"
        ]

        case write(
               resource,
               :destroy,
               key,
               statements,
               Map.merge(rid_vars, guard_vars),
               %{"destroyed" => :committed},
               changeset.filter
             ) do
          {:ok, _} -> :ok
          {:error, e} -> {:error, e}
        end
      end
    end

    defp all_fields(resource, pk) do
      resource
      |> Ash.Resource.Info.attributes()
      |> Enum.map(& &1.name)
      |> Enum.reject(&(&1 == pk))
    end

    # The table travels as a param; the key keeps its type (`<string>`
    # for string/UUID keys — 3.0.5 coerces UUID-shaped strings otherwise).
    defp record_id(resource, pk, value) do
      attr = Ash.Resource.Info.attribute(resource, pk)

      case Codec.encode(attr, value) do
        {:ok, :none} ->
          {:error, QueryFailed.exception(resource: resource, reason: :nil_primary_key)}

        {:ok, {kind, wire}}
        when kind in [:str, :plain] and (is_binary(wire) or is_integer(wire)) ->
          {:ok, "type::record(<string>$__tb, #{Codec.param_sql(kind, "__pk")})",
           %{"__tb" => table(resource), "__pk" => wire}}

        {:ok, _other} ->
          {:error,
           QueryFailed.exception(resource: resource, reason: {:unsupported_primary_key_type, pk})}

        {:error, e} ->
          {:error, QueryFailed.exception(resource: resource, reason: e)}
      end
    end

    defp guard(_resource, _pk, nil), do: {:ok, "", %{}}
    defp guard(_resource, _pk, %Ash.Filter{expression: nil}), do: {:ok, "", %{}}

    defp guard(resource, pk, filter) do
      case Filter.to_where(filter, pk, "g") do
        {:ok, {"", _}} -> {:ok, "", %{}}
        {:ok, {w, params}} -> {:ok, " WHERE " <> w, params}
        {:error, e} -> {:error, QueryFailed.exception(resource: resource, reason: e)}
      end
    end

    defp sets(resource, _pk, record, fields) do
      fields
      |> Enum.with_index()
      |> Enum.reduce_while({:ok, [], %{}}, fn {name, i}, {:ok, parts, vars} ->
        attr = Ash.Resource.Info.attribute(resource, name)
        col = Atom.to_string(attr.source || attr.name)

        cond do
          col == "id" or not DrinkMeAlice.identifier?(col) ->
            {:halt,
             {:error, QueryFailed.exception(resource: resource, reason: {:field_name, name})}}

          true ->
            case Codec.encode(attr, Map.get(record, name)) do
              {:ok, :none} ->
                {:cont, {:ok, ["#{col} = NONE" | parts], vars}}

              {:ok, {kind, wire}} ->
                p = "s#{i}"

                {:cont,
                 {:ok, ["#{col} = #{Codec.param_sql(kind, p)}" | parts], Map.put(vars, p, wire)}}

              {:error, e} ->
                {:halt, {:error, QueryFailed.exception(resource: resource, reason: e)}}
            end
        end
      end)
      |> case do
        {:ok, parts, vars} -> {:ok, parts |> Enum.reverse() |> Enum.join(", "), vars}
        e -> e
      end
    end

    defp write(resource, action, key, statements, vars, verdicts, filter \\ nil) do
      pk = primary_key!(resource)

      with {:ok, tx} <- Tx.build(statements, vars) do
        case Tx.run(conn(resource), tx, verdicts, deadline_ms: deadline_ms(resource)) do
          {:committed, %{"row" => row} = d} when is_map(row) ->
            decode_row(resource, pk, Map.put(row, "__pk", d["pk"]))

          {:committed, _d} ->
            {:ok, :done}

          {:existing, _d} ->
            {:ok, :done}

          {:rejected, %Error{} = e} ->
            {:error, rejection(resource, action, pk, filter, e)}

          {:unknown, _tx} ->
            {:error,
             OutcomeUnknown.exception(resource: resource, action_type: action, primary_key: key)}

          {:unavailable, %Error{} = e} ->
            {:error, Unavailable.exception(resource: resource, reason: e.detail || e.code)}
        end
      else
        {:error, e} -> {:error, QueryFailed.exception(resource: resource, reason: e)}
      end
    end

    # Qualified statement aborts carry the server's message. Map the ones
    # Ash has a vocabulary for; everything else is a typed query failure.
    defp rejection(resource, action, pk, filter, %Error{detail: {:tx_aborted, msg}})
         when is_binary(msg) do
      cond do
        String.contains?(msg, "bs:stale") ->
          StaleRecord.exception(resource: resource, filter: filter)

        action == :create and String.contains?(msg, "already exists") ->
          InvalidChanges.exception(fields: [pk], message: "has already been taken")

        String.contains?(msg, "already contains") ->
          InvalidChanges.exception(
            fields: identity_fields(resource, msg),
            message: "has already been taken"
          )

        true ->
          QueryFailed.exception(resource: resource, reason: {:statement_err, short(msg)})
      end
    end

    defp rejection(resource, _action, _pk, _filter, %Error{} = e),
      do: QueryFailed.exception(resource: resource, reason: {e.code, e.detail})

    # Identity index names are `<table>_<identity>_uq` (see Schema).
    defp identity_fields(resource, msg) do
      table = table(resource)

      resource
      |> Ash.Resource.Info.identities()
      |> Enum.find_value([], fn id ->
        if String.contains?(msg, "`#{table}_#{id.name}_uq`"), do: id.keys
      end)
    end

    defp short(msg) when is_binary(msg), do: String.slice(msg, 0, 160)
    defp short(other), do: inspect(other) |> String.slice(0, 160)
  end
end
