if Code.ensure_loaded?(Xandra) do
  defmodule EctoXandra.Connection do
    @moduledoc false

    alias Xandra.Prepared
    alias Ecto.Query.{BooleanExpr, QueryExpr, LimitExpr}

    @behaviour Ecto.Adapters.SQL.Connection

    ## Connection

    @impl true
    def child_spec(opts) do
      repo = Keyword.fetch!(opts, :repo)

      opts =
        if keyspace = Keyword.get(opts, :keyspace) do
          Keyword.put(opts, :after_connect, &Xandra.execute!(&1, "USE #{keyspace}"))
        else
          opts
        end

      Supervisor.child_spec({Xandra.Cluster, opts}, id: repo)
    end

    ## Execution

    @impl true
    def prepare_execute(cluster, _name, sql, params, opts) do
      with {:ok, %Prepared{} = prepared} <- Xandra.Cluster.prepare(cluster, sql, opts) do
        execute(cluster, prepared, params, opts)
      end
    end

    @impl true
    def query(cluster, sql, params, opts) do
      result = Xandra.Cluster.execute(cluster, sql, params, opts)

      case result do
        {:ok, %Xandra.Void{}} ->
          {:ok, %{rows: nil, num_rows: 1}}

        {:ok, %Xandra.Page{paging_state: nil} = page} ->
          {:ok, process_page(page)}

        {:ok, %Xandra.SchemaChange{} = schema_change} ->
          {:ok, schema_change}

        {:error, error} ->
          {:error, error}
      end
    end

    @impl true
    def query_many(_cluster, _sql, _params, _opts) do
      raise RuntimeError, "query_many is not supported in the Xandra adapter"
    end

    @impl true
    def execute(cluster, query, params, opts) do
      try do
        stream = Xandra.Cluster.stream_pages!(cluster, query, params, opts)

        result =
          Enum.reduce_while(stream, %{rows: [], num_rows: 0}, fn
            %Xandra.Void{}, _acc ->
              {:halt, %{rows: nil, num_rows: 1}}

            %Xandra.SchemaChange{}, _acc ->
              {:halt, %{rows: nil, num_rows: 1}}

            %Xandra.Page{} = page, %{rows: rows, num_rows: num_rows} ->
              %{rows: new_rows, num_rows: new_num_rows} = process_page(page)
              {:cont, %{rows: rows ++ new_rows, num_rows: num_rows + new_num_rows}}
          end)

        {:ok, query, result}
      rescue
        err ->
          {:error, err}
      end
    end

    @impl true
    def stream(_, _, _, _), do: raise("not implemented")

    @impl true
    def explain_query(_, _, _, _), do: raise("not implemented")

    ## Query

    @impl true
    def to_constraints(_, _), do: []

    @impl true
    def all(query, as_prefix \\ []) do
      sources = create_names(query, as_prefix)
      from = from(query, sources)
      select = select(query, sources)
      where = where(query, sources)
      order_by = order_by(query, sources)
      limit = limit(query, sources)

      [select, from, where, order_by, limit]
    end

    @impl true
    def insert(prefix, table, headers, rows, _on_conflict, _returning, opts)
        when prefix in [nil, ""] do
      keys = headers |> Enum.join(", ")
      values = rows |> Enum.map(&Enum.map_join(&1, ", ", fn _ -> "?" end))

      "INSERT INTO #{quote_table(prefix, table)} (#{keys}) VALUES (#{values}) #{insert_suffix(opts)}"
    end

    @impl true
    def update(prefix, table, fields, filters, _returning) do
      "UPDATE #{quote_table(prefix, table)} SET #{set(fields)} WHERE #{where(filters)}"
    end

    @impl true
    def delete(prefix, table, filters, _returning) do
      "DELETE FROM #{quote_table(prefix, table)} WHERE #{where(filters)}"
    end

    @impl true
    def update_all(_), do: raise("not implemented")

    @impl true
    def delete_all(query) do
      sources = create_names(query, [])
      from = from(query, sources)
      where = where(query, sources)
      ["DELETE", from, where]
    end

    ## DDL

    alias Ecto.Migration.Table

    defp column_definitions(columns) do
      columns
      |> Enum.map(&column_definition/1)
      |> Enum.join(", ")
    end

    defp column_definition({:add, name, type, _opts}) do
      "#{quote_name(name)} #{EctoXandra.xandra_type(type)}"
    end

    defp column_definition({:remove, name}) do
      "#{quote_name(name)}"
    end

    defp column_operations(columns) do
      {add, remove} =
        Enum.reduce(columns, {[], []}, fn
          {:add, _name, _type, _opts} = column, {add, remove} -> {[column | add], remove}
          {:remove, _name} = column, {add, remove} -> {add, [column | remove]}
        end)

      [
        column_operations(:add, add),
        column_operations(:remove, remove)
      ]
      |> Enum.reject(&is_nil/1)
      |> List.flatten()
    end

    defp column_operations(_op, []), do: nil

    defp column_operations(:add, columns) do
      "ADD (#{column_definitions(columns)})"
    end

    defp column_operations(:remove, columns) do
      "DROP (#{column_definitions(columns)})"
    end

    defp key_definitions(columns) do
      primary_keys =
        for {_, name, _, opts} <- columns,
            opts[:primary_key],
            do: name

      partition_keys =
        for {_, name, _, opts} <- columns,
            opts[:partition_key],
            do: name

      clustering_keys =
        for {_, name, _, opts} <- columns,
            opts[:clustering_key],
            do: name

      case {primary_keys, clustering_keys} do
        {[], []} ->
          "PRIMARY KEY ((#{Enum.join(partition_keys, ", ")}))"

        {[], _} ->
          "PRIMARY KEY ((#{Enum.join(partition_keys, ", ")}), #{Enum.join(clustering_keys, ", ")})"

        _ ->
          "PRIMARY KEY ((#{Enum.join(primary_keys, ", ")}))"
      end
    end

    defp ordering_bys(columns) do
      ordering_bys =
        for {_, name, _, opts} <- columns,
            opts[:clustering_key] && opts[:ordering_by],
            do: {name, ordering_by(opts[:ordering_by])}

      case ordering_bys do
        [] ->
          ""

        _ ->
          ordering_bys =
            ordering_bys
            |> Enum.map(fn {name, order} -> "#{name} #{order}" end)
            |> Enum.join(", ")

          " WITH CLUSTERING ORDER BY (#{ordering_bys})"
      end
    end

    defp ordering_by(:asc), do: "ASC"
    defp ordering_by(:desc), do: "DESC"

    @impl true
    def execute_ddl({command, %Table{} = table, columns})
        when command in [:create, :create_if_not_exists] do
      table_structure = column_definitions(columns) <> ", " <> key_definitions(columns)
      ordering_bys = ordering_bys(columns)

      if command == :create_if_not_exists do
        "CREATE TABLE IF NOT EXISTS #{quote_table(table.prefix, table.name)} (#{table_structure})" <>
          ordering_bys
      else
        "CREATE TABLE #{quote_table(table.prefix, table.name)} (#{table_structure})" <>
          ordering_bys
      end
    end

    @impl true
    def execute_ddl({command, %Table{} = table, _mode})
        when command in [:drop, :drop_if_exists] do
      if command == :drop_if_exists do
        "DROP TABLE IF EXISTS #{quote_table(table.prefix, table.name)}"
      else
        "DROP TABLE #{quote_table(table.prefix, table.name)}"
      end
    end

    @impl true
    def execute_ddl({:alter, %Table{} = table, columns}) do
      columns
      |> column_operations()
      |> Enum.map(&"ALTER TABLE #{quote_table(table.prefix, table.name)} #{&1}")
    end

    @impl true
    def execute_ddl(string) when is_binary(string), do: [string]

    @impl true
    def ddl_logs(_), do: []

    @impl true
    def table_exists_query(table),
      do: "SELECT table_name FROM system_schema.tables WHERE table_name = '#{table}'"

    ## Query generation helpers

    binary_ops = [
      ==: " = ",
      !=: " != ",
      <=: " <= ",
      >=: " >= ",
      <: " < ",
      >: " > ",
      +: " + ",
      -: " - ",
      *: " * ",
      /: " / ",
      and: " AND ",
      or: " OR ",
      like: " LIKE "
    ]

    @binary_ops Keyword.keys(binary_ops)

    Enum.map(binary_ops, fn {op, str} ->
      defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
    end)

    defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

    defp set(fields) do
      fields
      |> Enum.map_join(", ", fn
        {k, {:add, _}} -> "#{k} = #{k} + ?"
        {k, {:remove, _}} -> "#{k} = #{k} - ?"
        {k, _} -> "#{k} = ?"
      end)
    end

    defp insert_suffix(opts) do
      suffix =
        case Keyword.get(opts, :overwrite, true) do
          true ->
            []

          _ ->
            [" IF NOT EXISTS"]
        end

      suffix =
        case Keyword.get(opts, :ttl, nil) do
          nil -> suffix
          seconds when is_number(seconds) -> suffix ++ [" USING TTL #{seconds}"]
        end

      case Keyword.get(opts, :timestamp, nil) do
        nil ->
          suffix

        epoch_in_microseconds when is_number(epoch_in_microseconds) ->
          suffix ++ [" AND TIMESTAMP #{epoch_in_microseconds}"]
      end
    end

    defp select(%{select: %{fields: fields}} = query, sources) do
      ["SELECT ", select(fields, sources, query)]
    end

    defp select([], _sources, _query),
      do: "TRUE"

    defp select(fields, sources, query) do
      intersperse_map(fields, ", ", fn
        {:&, _, [idx]} ->
          case elem(sources, idx) do
            {source, _, nil} ->
              error!(
                query,
                "Cassandra does not support selecting all fields from #{source} without a schema. " <>
                  "Please specify a schema or specify exactly which fields you want to select"
              )

            {_, source, _} ->
              source
          end

        {key, value} ->
          [expr(value, sources, query), " AS ", quote_name(key)]

        value ->
          expr(value, sources, query)
      end)
    end

    defp from(%{from: %{source: {from, _schema}}}, _sources) do
      [" FROM ", from]
    end

    defp where(%{wheres: wheres} = query, sources) do
      boolean(" WHERE ", wheres, sources, query)
    end

    defp where(filters) when is_list(filters) do
      filters
      |> Enum.map_join(" and ", fn {k, _} -> "#{k} = ?" end)
    end

    defp order_by(%{order_bys: []}, _sources), do: []

    defp order_by(%{order_bys: order_bys} = query, sources) do
      [
        " ORDER BY "
        | intersperse_map(order_bys, ", ", fn %QueryExpr{expr: expr} ->
            intersperse_map(expr, ", ", &order_by_expr(&1, sources, query))
          end)
      ]
    end

    defp order_by_expr({dir, expr}, sources, query) do
      str = expr(expr, sources, query)

      case dir do
        :asc -> str
        :desc -> [str | " DESC"]
        _ -> error!(query, "#{dir} is not supported in ORDER BY in Cassandra")
      end
    end

    defp limit(%{limit: nil}, _sources), do: []

    defp limit(%{limit: %LimitExpr{expr: expr}} = query, sources) do
      [" LIMIT " | expr(expr, sources, query)]
    end

    defp boolean(_name, [], _sources, _query), do: []

    defp boolean(name, [%{expr: expr, op: op} | query_exprs], sources, query) do
      [
        name,
        Enum.reduce(query_exprs, {op, paren_expr(expr, sources, query)}, fn
          %BooleanExpr{expr: expr, op: op}, {op, acc} ->
            {op, [acc, operator_to_boolean(op) | paren_expr(expr, sources, query)]}

          %BooleanExpr{expr: expr, op: op}, {_, acc} ->
            {op, [?(, acc, ?), operator_to_boolean(op) | paren_expr(expr, sources, query)]}
        end)
        |> elem(1)
      ]
    end

    defp operator_to_boolean(:and), do: " AND "
    defp operator_to_boolean(:or), do: " OR "

    defp paren_expr(expr, sources, query) do
      [expr(expr, sources, query)]
    end

    defp expr(%Ecto.Query.Tagged{value: value}, sources, query) do
      expr(value, sources, query)
    end

    defp expr({:^, [], [_ix]}, _sources, _query) do
      '?'
    end

    defp expr({{:., _, [{:&, _, [_idx]}, field]}, _, []}, _sources, _query)
         when is_atom(field) do
      [quote_name(field)]
    end

    defp expr({:&, _, [idx]}, sources, _query) do
      {_, source, _} = elem(sources, idx)
      source
    end

    defp expr({:in, _, [_left, []]}, _sources, _query) do
      "false"
    end

    defp expr({:in, _, [left, right]}, sources, query) when is_list(right) do
      args = intersperse_map(right, ?,, &expr(&1, sources, query))
      [expr(left, sources, query), " IN (", args, ?)]
    end

    defp expr({:in, _, [_, {:^, _, [_, 0]}]}, _sources, _query) do
      "false"
    end

    defp expr({:in, _, [left, {:^, _, [_, length]}]}, sources, query) do
      args = Enum.intersperse(List.duplicate(??, length), ?,)
      [expr(left, sources, query), " IN (", args, ?)]
    end

    defp expr({:in, _, [left, right]}, sources, query) do
      [expr(left, sources, query), " = ANY(", expr(right, sources, query), ?)]
    end

    defp expr({:is_nil, _, [arg]}, sources, query) do
      [expr(arg, sources, query) | " IS NULL"]
    end

    defp expr({:not, _, [expr]}, sources, query) do
      ["NOT (", expr(expr, sources, query), ?)]
    end

    defp expr({:fragment, _, [kw]}, _sources, query) when is_list(kw) or tuple_size(kw) == 3 do
      error!(query, "Cassandra adapter does not support keyword or interpolated fragments")
    end

    defp expr({:fragment, _, parts}, sources, query) do
      Enum.map(parts, fn
        {:raw, part} -> part
        {:expr, expr} -> expr(expr, sources, query)
      end)
    end

    defp expr({:{}, _, elems}, sources, query) do
      [?(, intersperse_map(elems, ?,, &expr(&1, sources, query)), ?)]
    end

    defp expr({:count, _, []}, _sources, _query), do: "count(*)"

    defp expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
      {modifier, args} =
        case args do
          [rest, :distinct] -> {"DISTINCT ", [rest]}
          _ -> {[], args}
        end

      case handle_call(fun, length(args)) do
        {:binary_op, op} ->
          [left, right] = args
          [op_to_binary(left, sources, query), op | op_to_binary(right, sources, query)]

        {:fun, fun} ->
          [fun, ?(, modifier, intersperse_map(args, ", ", &expr(&1, sources, query)), ?)]
      end
    end

    defp expr(list, _sources, query) when is_list(list) do
      error!(query, "Array type is not supported by Cassandra")
    end

    defp expr(%Decimal{} = decimal, _sources, _query) do
      Decimal.to_string(decimal, :normal)
    end

    defp expr(nil, _sources, _query), do: "NULL"
    defp expr(true, _sources, _query), do: "TRUE"
    defp expr(false, _sources, _query), do: "FALSE"

    defp expr(literal, _sources, _query) when is_binary(literal) do
      [?', escape_string(literal), ?']
    end

    defp expr(literal, _sources, _query) when is_integer(literal) do
      Integer.to_string(literal)
    end

    defp expr(literal, _sources, _query) when is_float(literal) do
      # Cassandra doesn't support float cast
      ["(0 + ", Float.to_string(literal), ?)]
    end

    defp op_to_binary({op, _, [_, _]} = expr, sources, query) when op in @binary_ops,
      do: paren_expr(expr, sources, query)

    defp op_to_binary({:is_nil, _, [_]} = expr, sources, query),
      do: paren_expr(expr, sources, query)

    defp op_to_binary(expr, sources, query),
      do: expr(expr, sources, query)

    defp create_names(%{sources: sources}, as_prefix) do
      create_names(sources, 0, tuple_size(sources), as_prefix) |> List.to_tuple()
    end

    defp create_names(sources, pos, limit, as_prefix) when pos < limit do
      [create_name(sources, pos, as_prefix) | create_names(sources, pos + 1, limit, as_prefix)]
    end

    defp create_names(_sources, pos, pos, as_prefix) do
      [as_prefix]
    end

    defp create_name(sources, pos, as_prefix) do
      case elem(sources, pos) do
        {:fragment, _, _} ->
          {nil, as_prefix ++ [?f | Integer.to_string(pos)], nil}

        {table, schema, prefix} ->
          name = as_prefix ++ [create_alias(table) | Integer.to_string(pos)]
          {quote_table(prefix, table), name, schema}

        %Ecto.SubQuery{} ->
          {nil, as_prefix ++ [?s | Integer.to_string(pos)], nil}
      end
    end

    defp create_alias(<<first, _rest::binary>>) when first in ?a..?z when first in ?A..?Z do
      first
    end

    defp create_alias(_) do
      ?t
    end

    defp quote_name(name) when is_atom(name),
      do: quote_name(Atom.to_string(name))

    defp quote_name(name) do
      [name]
    end

    defp quote_table(nil, name), do: quote_table(name)
    defp quote_table(prefix, name), do: [quote_table(prefix), ?., quote_table(name)]

    defp quote_table(name) when is_atom(name),
      do: quote_table(Atom.to_string(name))

    defp quote_table(name) do
      [name]
    end

    defp intersperse_map(list, separator, mapper, acc \\ [])

    defp intersperse_map([], _separator, _mapper, acc),
      do: acc

    defp intersperse_map([elem], _separator, mapper, acc),
      do: [acc | mapper.(elem)]

    defp intersperse_map([elem | rest], separator, mapper, acc),
      do: intersperse_map(rest, separator, mapper, [acc, mapper.(elem), separator])

    defp escape_string(value) when is_binary(value) do
      value
      |> :binary.replace("'", "''", [:global])
      |> :binary.replace("\\", "\\\\", [:global])
    end

    defp error!(nil, message) do
      raise ArgumentError, message
    end

    defp error!(query, message) do
      raise Ecto.QueryError, query: query, message: message
    end

    ## Result handling

    defp process_page(%Xandra.Page{columns: [{_, _, "[applied]", _} | _], content: content}) do
      rows =
        content
        |> Enum.reject(&match?([false | _], &1))
        |> Enum.map(fn [_ | row] -> row end)

      %{rows: rows, num_rows: length(rows)}
    end

    defp process_page(%Xandra.Page{
           columns: [{_, _, "system.count" <> _, _} | _],
           content: [[count]]
         }) do
      %{rows: [[count]], num_rows: 1}
    end

    defp process_page(%Xandra.Page{columns: [{_, _, "count" <> _, _} | _], content: [[count]]}) do
      %{rows: [[count]], num_rows: 1}
    end

    defp process_page(%Xandra.Page{content: content}) do
      %{rows: content, num_rows: length(content)}
    end
  end
end
