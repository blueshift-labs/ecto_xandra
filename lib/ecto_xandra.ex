Code.ensure_loaded?(EctoXandra.Types.Map)
Code.ensure_loaded?(EctoXandra.Types.Json)
Code.ensure_loaded?(EctoXandra.Types.List)
Code.ensure_loaded?(EctoXandra.Types.Set)

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

  use Ecto.Adapters.SQL, driver: :xandra

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
    [{^repo, repo_opts}] = :ets.lookup(:ecto_xandra_opts, repo)

    Ecto.Adapters.SQL.execute(
      adapter_meta,
      query_meta,
      query,
      params,
      Keyword.merge(repo_opts, opts ++ [uuid_format: :binary])
    )
  end

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?(), do: false

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_, _, _), do: raise("not implemented")

  defp prepare_values(schema, params) do
    for source <- Keyword.keys(params) do
      field = source_field(schema, source)
      ecto_type = schema.__schema__(:type, field)
      {xandra_type(ecto_type) |> to_string(), source_value(params[source])}
    end
  end

  defp source_field(schema, source) do
    schema.__schema__(:fields)
    |> Enum.find(fn
      ^source -> true
      field -> schema.__schema__(:field_source, field) == source
    end)
  end

  defp source_value({:add, source}), do: source
  defp source_value({:remove, source}), do: source
  defp source_value(source), do: source

  def xandra_type(:id), do: :bigint
  def xandra_type(:binary_id), do: :uuid
  def xandra_type(:integer), do: :int
  def xandra_type(:string), do: :text
  def xandra_type(:binary), do: :blob

  def xandra_type(t) when t in [:utc_datetime, :utc_datetime_usec], do: :timestamp

  def xandra_type({:parameterized, type, opts}) do
    type.type(opts)
  end

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
