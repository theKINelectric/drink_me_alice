if Code.ensure_loaded?(Ash.DataLayer) do
  defmodule DrinkMeAlice.Ash.Codec do
    @moduledoc """
    Attribute ↔ SurrealDB value mapping for the Ash data layer, keyed on
    `Ash.Type.storage_type/2`.

    | storage type                               | Surreal field | written as                          |
    |--------------------------------------------|---------------|-------------------------------------|
    | `:string` `:citext` `:uuid` `:binary_id`   | `string`      | exact string (`DrinkMeAlice.exact_string/1`) |
    | `:naive_datetime*` `:date` `:time*`        | `string`      | exact ISO-8601 string               |
    | `:integer`                                 | `int`         | `$p` (i64)                          |
    | `:boolean`                                 | `bool`        | `$p`                                |
    | `:utc_datetime` `:utc_datetime_usec`       | `datetime`    | `<datetime>$p`                      |
    | `:decimal`                                 | `decimal`     | `<decimal>$p` (decimal string)      |
    | `:float`                                   | `float`       | `<float>$p` (float string)          |
    | `:map`                                     | `string`      | exact canonical JSON text           |
    | `{:array, scalar}`                         | `array<T>`    | element-wise as above               |

    Why: SurrealDB 3.0.5 coerces typed-looking JSON strings — date-shaped
    strings become datetimes and do not survive a `<string>` cast (see
    `DrinkMeAlice.exact_string/1`) — so every string travels in the exact form.
    The outbound encoder (`DrinkMeAlice.Json.encode/1`) carries only
    canonical terms, so floats, decimals and datetimes travel as strings
    cast in SurrealQL. Maps are stored as canonical JSON text rather than
    as objects: an object's nested strings would be coerced, and there is
    no server-side JSON parse to undo it; the cost is that map contents
    are not filterable. `nil` is written as the literal `NONE`: a JSON
    `null` is rejected by `option<T>` fields.

    Anything else is `{:error, {:unsupported_type, name, storage}}` — at
    schema generation and at write time — rather than a lossy guess.
    """

    # storage => {surreal field type, wire kind}
    @scalar %{
      string: {"string", :str},
      citext: {"string", :str},
      ci_string: {"string", :str},
      uuid: {"string", :str},
      binary_id: {"string", :str},
      naive_datetime: {"string", :str},
      naive_datetime_usec: {"string", :str},
      date: {"string", :str},
      time: {"string", :str},
      time_usec: {"string", :str},
      integer: {"int", :plain},
      boolean: {"bool", :plain},
      utc_datetime: {"datetime", {:cast, "datetime"}},
      utc_datetime_usec: {"datetime", {:cast, "datetime"}},
      decimal: {"decimal", {:cast, "decimal"}},
      float: {"float", {:cast, "float"}}
    }

    @type kind :: :str | :plain | {:cast, binary()} | :json | {:array, kind()}

    @doc "The storage type for an attribute."
    def storage(%{type: type, constraints: constraints}),
      do: Ash.Type.storage_type(type, constraints || [])

    @doc "The wire kind for a storage type, or `:error`."
    @spec wire_kind(term()) :: {:ok, kind()} | :error
    def wire_kind(:map), do: {:ok, :json}

    def wire_kind({:array, inner}) do
      case Map.fetch(@scalar, inner) do
        {:ok, {_t, k}} -> {:ok, {:array, k}}
        :error -> :error
      end
    end

    def wire_kind(storage) do
      case Map.fetch(@scalar, storage) do
        {:ok, {_t, k}} -> {:ok, k}
        :error -> :error
      end
    end

    @doc """
    The SurrealQL expression reading param `name` of `kind` back into the
    stored value.
    """
    @spec param_sql(kind(), binary()) :: binary()
    def param_sql(:str, name), do: DrinkMeAlice.exact_string_sql(name)
    def param_sql(:json, name), do: DrinkMeAlice.exact_string_sql(name)
    def param_sql(:plain, name), do: "$#{name}"
    def param_sql({:cast, c}, name), do: "<#{c}>$#{name}"
    def param_sql({:array, :str}, name), do: DrinkMeAlice.exact_string_list_sql(name)
    def param_sql({:array, :plain}, name), do: "$#{name}"
    def param_sql({:array, {:cast, c}}, name), do: "<array<#{c}>>$#{name}"

    @doc """
    The Surreal field type for an attribute, without `option<>`:
    `{:ok, "string"}` | `{:error, {:unsupported_type, name, storage}}`.
    """
    @spec field_type(map()) :: {:ok, binary()} | {:error, term()}
    def field_type(attr) do
      case storage(attr) do
        :map ->
          {:ok, "string"}

        {:array, inner} ->
          case Map.fetch(@scalar, inner) do
            {:ok, {t, _}} -> {:ok, "array<#{t}>"}
            :error -> {:error, {:unsupported_type, attr.name, storage(attr)}}
          end

        s ->
          case Map.fetch(@scalar, s) do
            {:ok, {t, _}} -> {:ok, t}
            :error -> {:error, {:unsupported_type, attr.name, s}}
          end
      end
    end

    @doc """
    Encode one attribute value for a write or a filter comparison:
    `{:ok, :none}` for nil (write the literal `NONE`), or
    `{:ok, {kind, wire_value}}` — pair with `param_sql/2`.
    """
    @spec encode(map(), term()) :: {:ok, :none | {kind(), term()}} | {:error, term()}
    def encode(_attr, nil), do: {:ok, :none}

    def encode(attr, value) do
      storage = storage(attr)

      with {:ok, kind} <- kind_or_error(attr, storage),
           {:ok, native} <- dump(attr, value),
           {:ok, wire} <- to_wire(storage, native) do
        {:ok, {kind, wire}}
      else
        {:error, {:unsupported_type, _, _}} = e -> e
        :error -> {:error, {:unsupported_type, attr.name, storage}}
        {:error, reason} -> {:error, {:encode, attr.name, reason}}
      end
    end

    defp kind_or_error(attr, storage) do
      case wire_kind(storage) do
        {:ok, k} -> {:ok, k}
        :error -> {:error, {:unsupported_type, attr.name, storage}}
      end
    end

    defp dump(attr, value) do
      case Ash.Type.dump_to_native(attr.type, value, attr.constraints || []) do
        {:ok, native} -> {:ok, native}
        :error -> {:error, :dump_failed}
        {:error, e} -> {:error, e}
      end
    end

    defp to_wire({:array, inner}, list) when is_list(list) do
      map_ok(list, &to_wire(inner, &1))
    end

    defp to_wire(:map, m) when is_map(m) do
      with {:ok, canonical} <- canonical_map(m),
           {:ok, json} <- DrinkMeAlice.Json.encode(canonical) do
        {:ok, DrinkMeAlice.exact_string(json)}
      else
        {:error, %{code: code}} -> {:error, code}
        other -> other
      end
    end

    defp to_wire(s, v) when s in [:string, :citext, :ci_string] and is_binary(v),
      do: {:ok, DrinkMeAlice.exact_string(v)}

    defp to_wire(s, v) when s in [:uuid, :binary_id] and is_binary(v),
      do: {:ok, DrinkMeAlice.exact_string(uuid_string(v))}

    defp to_wire(:integer, v) when is_integer(v), do: {:ok, v}
    defp to_wire(:boolean, v) when is_boolean(v), do: {:ok, v}

    defp to_wire(s, %DateTime{} = v) when s in [:utc_datetime, :utc_datetime_usec],
      do: {:ok, DateTime.to_iso8601(v)}

    defp to_wire(s, %NaiveDateTime{} = v) when s in [:utc_datetime, :utc_datetime_usec],
      do: {:ok, v |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_iso8601()}

    defp to_wire(s, %NaiveDateTime{} = v) when s in [:naive_datetime, :naive_datetime_usec],
      do: {:ok, DrinkMeAlice.exact_string(NaiveDateTime.to_iso8601(v))}

    defp to_wire(:date, %Date{} = v), do: {:ok, DrinkMeAlice.exact_string(Date.to_iso8601(v))}

    defp to_wire(s, %Time{} = v) when s in [:time, :time_usec],
      do: {:ok, DrinkMeAlice.exact_string(Time.to_iso8601(v))}

    defp to_wire(:decimal, %Decimal{} = v), do: {:ok, Decimal.to_string(v, :normal)}
    defp to_wire(:float, v) when is_float(v), do: {:ok, Float.to_string(v)}
    defp to_wire(:float, v) when is_integer(v), do: {:ok, Float.to_string(v * 1.0)}
    defp to_wire(_storage, _v), do: :error

    # Ecto's :uuid dump yields the 16 raw bytes; the store keeps the
    # canonical textual form.
    defp uuid_string(<<_::128>> = raw), do: raw |> Ecto.UUID.load() |> elem(1)
    defp uuid_string(s), do: s

    # Map values must be canonical for `DrinkMeAlice.Json.encode/1`: string
    # keys (atoms are stringified), binaries, i64 integers, booleans, nil,
    # lists and nested maps. Floats/structs are refused, never coerced.
    defp canonical_map(m) do
      Enum.reduce_while(m, {:ok, %{}}, fn {k, v}, {:ok, acc} ->
        with {:ok, k} <- canonical_key(k),
             {:ok, v} <- canonical_value(v) do
          {:cont, {:ok, Map.put(acc, k, v)}}
        else
          _ -> {:halt, {:error, :noncanonical_map}}
        end
      end)
    end

    defp canonical_key(k) when is_binary(k), do: {:ok, k}
    defp canonical_key(k) when is_atom(k), do: {:ok, Atom.to_string(k)}
    defp canonical_key(_), do: :error

    defp canonical_value(v) when is_binary(v) or is_integer(v) or is_boolean(v) or is_nil(v),
      do: {:ok, v}

    defp canonical_value(v) when is_map(v) and not is_struct(v), do: canonical_map(v)
    defp canonical_value(v) when is_list(v), do: map_ok(v, &canonical_value/1)
    defp canonical_value(_), do: :error

    @doc """
    Decode one stored field value back into the attribute's Ash value
    (`nil` for an absent field).
    """
    @spec decode(map(), term()) :: {:ok, term()} | {:error, term()}
    def decode(_attr, nil), do: {:ok, nil}

    def decode(attr, value) do
      storage = storage(attr)

      with {:ok, native} <- from_wire(storage, value),
           {:ok, cast} <- Ash.Type.cast_stored(attr.type, native, attr.constraints || []) do
        {:ok, cast}
      else
        :error -> {:error, {:decode, attr.name}}
        {:error, e} -> {:error, {:decode, attr.name, e}}
      end
    end

    defp from_wire({:array, inner}, list) when is_list(list),
      do: map_ok(list, &from_wire(inner, &1))

    # Stored JSON text is decoded under the connector's bounded profile.
    defp from_wire(:map, v) when is_binary(v) do
      case DrinkMeAlice.Json.decode(v) do
        {:ok, m} when is_map(m) -> {:ok, m}
        _ -> :error
      end
    end

    defp from_wire(s, v) when s in [:utc_datetime, :utc_datetime_usec] and is_binary(v) do
      case DateTime.from_iso8601(v) do
        {:ok, dt, _offset} -> {:ok, dt}
        _ -> :error
      end
    end

    defp from_wire(s, v) when s in [:naive_datetime, :naive_datetime_usec] and is_binary(v),
      do: ok_or_error(NaiveDateTime.from_iso8601(v))

    defp from_wire(:date, v) when is_binary(v), do: ok_or_error(Date.from_iso8601(v))

    defp from_wire(s, v) when s in [:time, :time_usec] and is_binary(v),
      do: ok_or_error(Time.from_iso8601(v))

    defp from_wire(:decimal, v) when is_binary(v) do
      case Decimal.parse(v) do
        {d, ""} -> {:ok, d}
        _ -> :error
      end
    end

    defp from_wire(:decimal, v) when is_integer(v), do: {:ok, Decimal.new(v)}
    defp from_wire(:float, v) when is_float(v), do: {:ok, v}
    defp from_wire(:float, v) when is_integer(v), do: {:ok, v * 1.0}
    defp from_wire(_storage, v), do: {:ok, v}

    defp ok_or_error({:ok, v}), do: {:ok, v}
    defp ok_or_error(_), do: :error

    defp map_ok(list, fun) do
      Enum.reduce_while(list, {:ok, []}, fn x, {:ok, acc} ->
        case fun.(x) do
          {:ok, y} -> {:cont, {:ok, [y | acc]}}
          other -> {:halt, other}
        end
      end)
      |> case do
        {:ok, acc} -> {:ok, Enum.reverse(acc)}
        other -> other
      end
    end
  end
end
