if Code.ensure_loaded?(Ash.DataLayer) do
  defmodule DrinkMeAlice.Ash.Schema do
    @moduledoc """
    SurrealQL DDL for resources using `DrinkMeAlice.Ash.DataLayer`,
    shaped as `DrinkMeAlice.Migrations` migrations.

        migrations = [
          DrinkMeAlice.Ash.Schema.migration(1, "crew", [MyApp.Crew.Seat, MyApp.Crew.Mission])
        ]
        :ok = DrinkMeAlice.Admin.migrate(root_config, migrations)

    Per resource: `DEFINE TABLE <t> SCHEMAFULL`, one `DEFINE FIELD` per
    non-primary-key attribute (`option<T>` when `allow_nil?`), and a
    `UNIQUE` index per identity. The primary key is the record id, not a
    field. Migrations are append-only: once a version is applied its
    statements are checksummed, so a changed resource needs a NEW
    migration (e.g. `DEFINE FIELD … OVERWRITE`), never an edited one.
    """

    alias DrinkMeAlice.Ash.{Codec, DataLayer}

    @doc "A migration map for `resources` (raises on an unsupported attribute type)."
    @spec migration(pos_integer(), binary(), [module()]) :: DrinkMeAlice.Migrations.migration()
    def migration(version, name, resources) do
      %{version: version, name: name, statements: Enum.flat_map(resources, &statements!/1)}
    end

    @doc "The DDL statements for one resource."
    @spec statements(module()) :: {:ok, [binary()]} | {:error, term()}
    def statements(resource) do
      table = DataLayer.table(resource)
      pk = DataLayer.primary_key!(resource)

      attrs =
        resource
        |> Ash.Resource.Info.attributes()
        |> Enum.reject(&(&1.name == pk))

      with :ok <- check_names(attrs),
           {:ok, fields} <- fields(table, attrs) do
        indexes =
          resource
          |> Ash.Resource.Info.identities()
          |> Enum.map(fn id ->
            cols = Enum.map_join(id.keys, ", ", &Atom.to_string(source(resource, &1)))
            "DEFINE INDEX #{table}_#{id.name}_uq ON TABLE #{table} FIELDS #{cols} UNIQUE"
          end)

        {:ok, ["DEFINE TABLE #{table} SCHEMAFULL" | fields] ++ indexes}
      end
    end

    @doc "Like `statements/1`, raising on error."
    def statements!(resource) do
      case statements(resource) do
        {:ok, s} -> s
        {:error, e} -> raise ArgumentError, "#{inspect(resource)}: #{inspect(e)}"
      end
    end

    @doc """
    DDL for a graph edge table (`TYPE RELATION`), for use with
    `DrinkMeAlice.Graph`. `fields` is `[{name, surreal_type}]`.
    """
    @spec edge_statements(binary(), binary(), binary(), [{binary(), binary()}]) :: [binary()]
    def edge_statements(edge, from_table, to_table, fields \\ []) do
      for t <- [edge, from_table, to_table] ++ Enum.map(fields, &elem(&1, 0)),
          not DrinkMeAlice.identifier?(t),
          do: raise(ArgumentError, "not an identifier: #{inspect(t)}")

      ["DEFINE TABLE #{edge} SCHEMAFULL TYPE RELATION IN #{from_table} OUT #{to_table}"] ++
        Enum.map(fields, fn {f, t} -> "DEFINE FIELD #{f} ON TABLE #{edge} TYPE #{t}" end)
    end

    defp check_names(attrs) do
      Enum.find_value(attrs, :ok, fn a ->
        name = Atom.to_string(a.source || a.name)

        cond do
          name == "id" -> {:error, {:reserved_field, a.name}}
          not DrinkMeAlice.identifier?(name) -> {:error, {:field_name, a.name}}
          true -> nil
        end
      end)
    end

    defp fields(table, attrs) do
      Enum.reduce_while(attrs, {:ok, []}, fn a, {:ok, acc} ->
        case Codec.field_type(a) do
          {:ok, t} ->
            t = if a.allow_nil?, do: "option<#{t}>", else: t
            name = Atom.to_string(a.source || a.name)
            {:cont, {:ok, ["DEFINE FIELD #{name} ON TABLE #{table} TYPE #{t}" | acc]}}

          {:error, e} ->
            {:halt, {:error, e}}
        end
      end)
      |> case do
        {:ok, acc} -> {:ok, Enum.reverse(acc)}
        e -> e
      end
    end

    defp source(resource, name) do
      attr = Ash.Resource.Info.attribute(resource, name)
      attr.source || attr.name
    end
  end
end
