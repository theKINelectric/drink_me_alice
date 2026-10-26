if Code.ensure_loaded?(Ash.DataLayer) do
  defmodule DrinkMeAlice.Ash.Filter do
    @moduledoc """
    Translate an Ash filter into a parameterized SurrealQL `WHERE` clause.

    Supported: `and`/`or`/`not`, boolean literals, and — on a plain
    attribute of the queried resource (no relationship path) compared with
    a literal — `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `is_nil`. Values
    are encoded by the attribute's own type (`DrinkMeAlice.Ash.Codec`)
    and always travel as bound params — never spliced into SQL text.

    The primary key lives in the record id, so a reference to it becomes
    `record::id(id)`.

    Anything else is refused: `supported?/2` answers the data layer's
    `can?(_, {:filter_expr, expr})`, so Ash reports an unsupported
    filter before a query is built, and `to_where/3` returns
    `{:error, {:unsupported_filter, _}}` as a second line of defence.
    Nothing is ever evaluated client-side over an unbounded fetch.
    """

    alias Ash.Query.{BooleanExpression, Not, Ref}
    alias Ash.Query.Operator
    alias DrinkMeAlice.Ash.Codec

    @comparisons %{
      Operator.Eq => "=",
      Operator.NotEq => "!=",
      Operator.GreaterThan => ">",
      Operator.GreaterThanOrEqual => ">=",
      Operator.LessThan => "<",
      Operator.LessThanOrEqual => "<="
    }

    # `a op b` read as `b flip(op) a` when the literal is on the left.
    @flipped %{"=" => "=", "!=" => "!=", ">" => "<", ">=" => "<=", "<" => ">", "<=" => ">="}

    @max_in 1_000

    @doc "True when `expr` (one filter node) can be pushed down."
    def supported?(_resource, %BooleanExpression{}), do: true
    def supported?(_resource, %Not{}), do: true
    def supported?(_resource, bool) when is_boolean(bool), do: true

    def supported?(_resource, %mod{left: l, right: r}) when is_map_key(@comparisons, mod),
      do: (comparable_ref?(l) and literal?(r)) or (literal?(l) and comparable_ref?(r))

    def supported?(_resource, %Operator.In{left: l, right: r}),
      do: comparable_ref?(l) and literal?(r)

    def supported?(_resource, %Operator.IsNil{left: l, right: r}),
      do: plain_ref?(l) and is_boolean(r)

    def supported?(_resource, _other), do: false

    defp plain_ref?(%Ref{relationship_path: [], attribute: %{name: _}}), do: true
    defp plain_ref?(_), do: false

    # Map attributes are stored as JSON text and arrays as arrays: neither
    # compares meaningfully with a scalar operator, so both are refused.
    defp comparable_ref?(%Ref{attribute: %{type: _} = attr} = ref) do
      plain_ref?(ref) and scalar_kind?(Codec.wire_kind(Codec.storage(attr)))
    end

    defp comparable_ref?(_), do: false

    defp scalar_kind?({:ok, :str}), do: true
    defp scalar_kind?({:ok, :plain}), do: true
    defp scalar_kind?({:ok, {:cast, _}}), do: true
    defp scalar_kind?(_), do: false

    defp literal?(%{__struct__: s}) when s in [Ref, BooleanExpression, Not], do: false
    defp literal?(%{__operator__?: true}), do: false
    defp literal?(%{__function__?: true}), do: false
    defp literal?(_), do: true

    @doc """
    `{:ok, {where_sql, params}}` for a filter (`{"", %{}}` when there is
    none), or `{:error, reason}`. `pk` is the primary-key attribute name;
    `prefix` namespaces the generated param names.
    """
    @spec to_where(Ash.Filter.t() | nil, atom(), binary()) ::
            {:ok, {binary(), map()}} | {:error, term()}
    def to_where(filter, pk, prefix \\ "f")

    def to_where(nil, _pk, _prefix), do: {:ok, {"", %{}}}
    def to_where(%Ash.Filter{expression: nil}, _pk, _prefix), do: {:ok, {"", %{}}}

    def to_where(%Ash.Filter{expression: expr}, pk, prefix) do
      case expr(expr, %{pk: pk, prefix: prefix, n: 0, params: %{}}) do
        {:ok, {t, _f}, st} -> {:ok, {t, st.params}}
        {:error, e} -> {:error, e}
      end
    end

    # Three-valued logic, as Ash and SQL define it: a comparison with an
    # absent (NONE) operand is UNKNOWN — neither true nor false — and a
    # WHERE keeps only rows that are TRUE. SurrealQL is two-valued (`NONE
    # <= 2` is true, `!(NONE = 1)` is true), so every node is translated
    # to a pair `{T, F}`: the condition for "definitely true" and for
    # "definitely false". NOT swaps them; AND/OR combine them per
    # Kleene's rules. The WHERE clause is T of the root.

    defp expr(true, st), do: {:ok, {"true", "false"}, st}
    defp expr(false, st), do: {:ok, {"false", "true"}, st}

    defp expr(%BooleanExpression{op: :and, left: l, right: r}, st) do
      with {:ok, {lt, lf}, st} <- expr(l, st),
           {:ok, {rt, rf}, st} <- expr(r, st) do
        {:ok, {"(#{lt} AND #{rt})", "(#{lf} OR #{rf})"}, st}
      end
    end

    defp expr(%BooleanExpression{op: :or, left: l, right: r}, st) do
      with {:ok, {lt, lf}, st} <- expr(l, st),
           {:ok, {rt, rf}, st} <- expr(r, st) do
        {:ok, {"(#{lt} OR #{rt})", "(#{lf} AND #{rf})"}, st}
      end
    end

    defp expr(%Not{expression: e}, st) do
      with {:ok, {t, f}, st} <- expr(e, st), do: {:ok, {f, t}, st}
    end

    # is_nil is never unknown.
    defp expr(%Operator.IsNil{left: %Ref{} = ref, right: nil?}, st) when is_boolean(nil?) do
      with {:ok, field, _attr} <- field(ref, st) do
        is_none = "(#{field} IS NONE)"
        not_none = "(#{field} IS NOT NONE)"
        {:ok, if(nil?, do: {is_none, not_none}, else: {not_none, is_none}), st}
      end
    end

    defp expr(%Operator.In{left: %Ref{} = ref, right: values}, st) do
      values = if is_struct(values, MapSet), do: MapSet.to_list(values), else: values

      cond do
        not is_list(values) ->
          {:error, {:unsupported_filter, :in_right}}

        length(values) > @max_in ->
          {:error, {:bounded_limit_exceeded, :filter_in_values}}

        true ->
          with {:ok, field, attr} <- field(ref, st),
               {:ok, kind} <- list_kind(attr),
               {:ok, wire} <- encode_list(attr, values) do
            {name, st} = param(st, wire)
            {:ok, when_present(field, "#{field} IN #{Codec.param_sql({:array, kind}, name)}"), st}
          end
      end
    end

    defp expr(%mod{left: l, right: r}, st) when is_map_key(@comparisons, mod) do
      op = Map.fetch!(@comparisons, mod)

      case {l, r} do
        {%Ref{} = ref, value} -> compare(ref, op, value, st)
        {value, %Ref{} = ref} -> compare(ref, Map.fetch!(@flipped, op), value, st)
      end
    end

    defp expr(other, _st), do: {:error, {:unsupported_filter, describe(other)}}

    # A predicate over a field that may be NONE: true/false only when the
    # field is present.
    defp when_present(field, pred),
      do: {"(#{field} IS NOT NONE AND #{pred})", "(#{field} IS NOT NONE AND !(#{pred}))"}

    defp compare(ref, op, value, st) do
      with {:ok, field, attr} <- field(ref, st) do
        case Codec.encode(attr, value) do
          {:ok, :none} ->
            # Comparing with nil is UNKNOWN for every row (SQL `= NULL`);
            # use is_nil to test for absence.
            {:ok, {"false", "false"}, st}

          {:ok, {kind, wire}} ->
            {name, st} = param(st, wire)
            {:ok, when_present(field, "#{field} #{op} #{Codec.param_sql(kind, name)}"), st}

          {:error, e} ->
            {:error, e}
        end
      end
    end

    defp encode_list(attr, values) do
      Enum.reduce_while(values, {:ok, []}, fn v, {:ok, acc} ->
        case Codec.encode(attr, v) do
          {:ok, {_cast, wire}} -> {:cont, {:ok, [wire | acc]}}
          {:ok, :none} -> {:halt, {:error, {:unsupported_filter, :nil_in_list}}}
          {:error, e} -> {:halt, {:error, e}}
        end
      end)
      |> case do
        {:ok, acc} -> {:ok, Enum.reverse(acc)}
        e -> e
      end
    end

    defp field(%Ref{relationship_path: [], attribute: %{name: name} = attr}, st) do
      cond do
        name == st.pk ->
          {:ok, "record::id(id)", attr}

        DrinkMeAlice.identifier?(Atom.to_string(source(attr))) ->
          {:ok, Atom.to_string(source(attr)), attr}

        true ->
          {:error, {:unsupported_filter, {:field_name, name}}}
      end
    end

    defp field(%Ref{} = ref, _st), do: {:error, {:unsupported_filter, {:ref, Ref.name(ref)}}}

    defp source(attr), do: Map.get(attr, :source) || attr.name

    defp param(st, wire) do
      name = "#{st.prefix}#{st.n}"
      {name, %{st | n: st.n + 1, params: Map.put(st.params, name, wire)}}
    end

    defp list_kind(attr) do
      case Codec.wire_kind(Codec.storage(attr)) do
        {:ok, k} when k == :str or k == :plain -> {:ok, k}
        {:ok, {:cast, _} = k} -> {:ok, k}
        _ -> {:error, {:unsupported_filter, {:in_on, attr.name}}}
      end
    end

    defp describe(%mod{}), do: mod
    defp describe(other), do: other
  end
end
