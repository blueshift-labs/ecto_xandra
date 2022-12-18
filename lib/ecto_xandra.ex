Code.ensure_loaded?(EctoXandra.Types.Map)
Code.ensure_loaded?(EctoXandra.Types.Json)
Code.ensure_loaded?(EctoXandra.Types.List)
Code.ensure_loaded?(EctoXandra.Types.Set)
Code.ensure_loaded?(EctoXandra.Types.Timestamp)

defmodule EctoXandra do
  defimpl String.Chars, for: Xandra.Simple do
    def to_string(simple) do
      inspect(simple)
    end
  end

  defimpl String.Chars, for: Xandra.Prepared do
    def to_string(prepared) do
      inspect(prepared)
    end
  end

  defimpl String.Chars, for: Xandra.Batch do
    def to_string(prepared) do
      inspect(prepared)
    end
  end

  alias EctoXandra.Types.{List, Set}
  alias EctoXandra.Types.Map, as: XMap

  @behaviour Ecto.Adapter.Storage
  @impl true
  def storage_up(opts) do
    keyspace = Keyword.fetch!(opts, :keyspace)

    Application.ensure_all_started(:xandra)

    {:ok, conn} =
      Xandra.start_link(Keyword.take(opts, [:nodes, :protocol_version, :log, :timeout]))

    stmt = """
    CREATE KEYSPACE IF NOT EXISTS #{keyspace}
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    AND durable_writes = true;
    """

    case Xandra.execute(conn, stmt) do
      {:ok, %Xandra.SchemaChange{effect: "CREATED"}} -> :ok
      {:ok, %Xandra.Void{}} -> {:error, :already_up}
      err -> err
    end
  end

  @impl true
  def storage_down(opts) do
    keyspace = Keyword.fetch!(opts, :keyspace)

    Application.ensure_all_started(:xandra)

    {:ok, conn} =
      Xandra.start_link(Keyword.take(opts, [:nodes, :protocol_version, :log, :timeout]))

    stmt = """
    DROP KEYSPACE IF EXISTS #{keyspace};
    """

    case Xandra.execute(conn, stmt) do
      {:ok, %Xandra.SchemaChange{effect: "DROPPED"}} -> :ok
      {:ok, %Xandra.Void{}} -> {:error, :already_down}
      err -> err
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_status(opts) do
    keyspace = Keyword.fetch!(opts, :keyspace)

    Application.ensure_all_started(:xandra)

    {:ok, conn} =
      Xandra.start_link(Keyword.take(opts, [:nodes, :protocol_version, :log, :timeout]))

    stmt = """
    USE #{keyspace};
    """

    case Xandra.execute(conn, stmt) do
      {:error, %Xandra.Error{reason: :invalid}} ->
        :down

      {:error, reason} ->
        {:error, reason}

      _ ->
        :up
    end
  end

  @impl true
  def in_transaction?(%{sql: EctoXandra.Connection}), do: true

  use Ecto.Adapters.SQL, driver: :xandra

  # decode from map if it embeds_many with unique primary_keys
  @impl true
  def loaders(
        {:map, _},
        {:parameterized, Ecto.Embedded, %Ecto.Embedded{cardinality: :many, unique: true}} = type
      ) do
    loader = fn value ->
      case Jason.decode!(value || "null") do
        %{} = data ->
          data = Map.values(data) |> Enum.sort_by(&Map.get(&1, "__index__"))
          Ecto.Type.embedded_load(type, data, :json)

        data ->
          Ecto.Type.embedded_load(type, data, :json)
      end
    end

    [loader]
  end

  def loaders({:map, _}, type),
    do: [&Ecto.Type.embedded_load(type, Jason.decode!(&1 || "null"), :json)]

  def loaders(:binary_id, type), do: [Ecto.UUID, type]
  def loaders(_, type), do: [type]

  @impl Ecto.Adapter.Schema
  def insert(
        %{repo: repo} = adapter_meta,
        %{source: source, prefix: prefix, schema: schema},
        params,
        {kind, conflict_params, _} = on_conflict,
        returning,
        opts
      ) do
    opts = put_source(opts, source)

    [{^repo, repo_opts}] = :ets.lookup(:ecto_xandra_opts, repo)

    {fields, _} = :lists.unzip(params)
    sql = @conn.insert(prefix, source, fields, [fields], on_conflict, returning, opts)
    values = prepare_values(schema, params)

    Ecto.Adapters.SQL.struct(
      adapter_meta,
      @conn,
      sql,
      :insert,
      source,
      [],
      values ++ conflict_params,
      kind,
      returning,
      Keyword.merge(repo_opts, opts)
    )
  end

  @impl Ecto.Adapter.Schema
  def update(
        %{repo: repo} = adapter_meta,
        %{source: source, prefix: prefix, schema: schema},
        fields,
        params,
        returning,
        opts
      ) do
    opts = put_source(opts, source)

    [{^repo, repo_opts}] = :ets.lookup(:ecto_xandra_opts, repo)

    sql = @conn.update(prefix, source, fields, params, returning)
    values = prepare_values(schema, fields ++ params)

    Ecto.Adapters.SQL.struct(
      adapter_meta,
      @conn,
      sql,
      :update,
      source,
      params,
      values,
      :raise,
      returning,
      Keyword.merge(repo_opts, opts)
    )
  end

  @impl Ecto.Adapter.Schema
  def delete(
        %{repo: repo} = adapter_meta,
        %{source: source, prefix: prefix, schema: schema},
        params,
        opts
      ) do
    opts = put_source(opts, source)

    [{^repo, repo_opts}] = :ets.lookup(:ecto_xandra_opts, repo)

    sql = @conn.delete(prefix, source, params, [])
    values = prepare_values(schema, params)

    Ecto.Adapters.SQL.struct(
      adapter_meta,
      @conn,
      sql,
      :delete,
      source,
      params,
      values,
      :raise,
      [],
      Keyword.merge(repo_opts, opts)
    )
  end

  @impl Ecto.Adapter.Queryable
  def execute(%{repo: repo} = adapter_meta, query_meta, query, params, opts) do
    case :ets.lookup(:ecto_xandra_opts, repo) do
      [{^repo, repo_opts}] ->
        Ecto.Adapters.SQL.execute(
          :named,
          adapter_meta,
          query_meta,
          query,
          params,
          Keyword.merge(repo_opts, opts ++ [uuid_format: :binary])
        )

      [] ->
        # debug
        Rollbax.report_message(:debug, "checking repo", %{
          go_uts: Ecto.Repo.Registry.lookup(UTX.GoUTSRepo),
          uts_c1: Ecto.Repo.Registry.lookup(UTX.UTSRepoC1),
          uts_c2: Ecto.Repo.Registry.lookup(UTX.UTSRepoC2),
          ecto_xandra_opts: :ets.tab2list(:ecto_xandra_opts)
        })
    end
  end

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?(), do: false

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_, _, fun), do: fun.()

  # this is to accommodate with schema migration
  defp prepare_values(Ecto.Migration.SchemaMigration = schema, params) do
    for source <- Keyword.keys(params) do
      field = source_field(schema, source)
      ecto_type = schema.__schema__(:type, field)

      case ecto_type do
        :integer ->
          {:bigint |> to_string(), source_value(ecto_type, params[source])}

        _ ->
          {xandra_type(ecto_type) |> to_string(), source_value(ecto_type, params[source])}
      end
    end
  end

  defp prepare_values(schema, params) do
    for source <- Keyword.keys(params) do
      field = source_field(schema, source)
      ecto_type = schema.__schema__(:type, field)
      {xandra_type(ecto_type) |> to_string(), source_value(ecto_type, params[source])}
    end
  end

  defp source_field(schema, source) do
    schema.__schema__(:fields)
    |> Enum.find(fn
      ^source -> true
      field -> schema.__schema__(:field_source, field) == source
    end)
  end

  # encode values into map if it embeds_many with unique primary_keys
  defp source_value(
         {:parameterized, Ecto.Embedded,
          %Ecto.Embedded{cardinality: :many, unique: true, ordered: ordered, related: schema}},
         values
       )
       when is_list(values) do
    primary_keys =
      schema.__schema__(:primary_key)
      |> Enum.map(&schema.__schema__(:field_source, &1))

    if primary_keys == [] do
      values =
        Enum.map(values, fn value ->
          value
          |> Enum.reject(&match?({_, nil}, &1))
          |> Enum.into(%{})
        end)

      Jason.encode!(values)
    else
      data =
        values
        |> Enum.with_index()
        |> Enum.reduce(%{}, fn {value, index}, acc ->
          key = primary_keys |> Enum.map(&Map.get(value, &1)) |> Enum.join("/")
          value = if ordered, do: Map.put(value, :__index__, index), else: value

          value =
            value
            |> Enum.reject(&match?({_, nil}, &1))
            |> Enum.into(%{})

          Map.put(acc, key, value)
        end)

      Jason.encode!(data)
    end
  end

  defp source_value({:parameterized, Ecto.Embedded, _}, %{} = value) do
    value
    |> Enum.reject(&match?({_, nil}, &1))
    |> Enum.into(%{})
    |> Jason.encode!()
  end

  defp source_value({:parameterized, Ecto.Embedded, _}, value), do: Jason.encode!(value)
  defp source_value(_, {:add, value}), do: value
  defp source_value(_, {:remove, value}), do: value
  defp source_value(_, %NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")
  defp source_value(_, value), do: value

  def xandra_type(:id), do: :bigint
  def xandra_type(:binary_id), do: :uuid
  def xandra_type(:integer), do: :int
  def xandra_type(:string), do: :text
  def xandra_type(:binary), do: :blob

  def xandra_type(t) when t in [:naive_datetime, :utc_datetime, :utc_datetime_usec],
    do: :timestamp

  def xandra_type({:parameterized, Ecto.Embedded, _}), do: :text

  def xandra_type({:parameterized, type, opts}) do
    type.type(opts)
  end

  def xandra_type({:list, opts}), do: List.type(opts)
  def xandra_type({:array, opts}), do: List.type(opts)
  def xandra_type({:set, opts}), do: Set.type(opts)
  def xandra_type({:map, opts}), do: XMap.type(opts)

  def xandra_type(ecto_type) do
    cond do
      is_atom(ecto_type) and function_exported?(ecto_type, :type, 0) ->
        ecto_type.type()

      true ->
        ecto_type
    end
  end

  def put_source(opts, source) when is_binary(source), do: Keyword.put(opts, :source, source)
  def put_source(opts, _), do: opts
end
