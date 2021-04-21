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

  use Ecto.Adapters.SQL, driver: :xandra

  @behaviour Ecto.Adapter.Storage

  @impl Ecto.Adapter.Storage
  def storage_up(opts) do
    keyspace =
      Keyword.fetch!(opts, :keyspace) || raise ":keyspace is nil in repository configuration"

    opts = Keyword.delete(opts, :keyspace)

    replication = Keyword.get(opts, :replication, class: SimpleStrategy, replication_factor: 1)
    opts = Keyword.delete(opts, :replication)

    create_command = """
    CREATE KEYSPACE IF NOT EXISTS #{keyspace}
    WITH replication = {#{format_replication(replication)}}
    AND durable_writes = true;
    """

    case run_query(create_command, opts) do
      {:ok, _} ->
        :ok

      {:error, error} ->
        {:error, Exception.message(error)}

      {:exit, exit} ->
        {:error, exit_to_exception(exit)}
    end
  end

  defp format_replication(replication) do
    replication
    |> Enum.map_join(", ", fn {k, v} -> "'#{k}'': '#{v}'" end)
  end

  @impl Ecto.Adapter.Storage
  def storage_down(opts) do
    keyspace =
      Keyword.fetch!(opts, :keyspace) || raise ":keyspace is nil in repository configuration"

    opts = Keyword.delete(opts, :keyspace)

    command = "DROP KEYSPACE `#{keyspace}`"

    case run_query(command, opts) do
      {:ok, _} ->
        :ok

      {:error, error} ->
        {:error, Exception.message(error)}

      {:exit, exit} ->
        {:error, exit_to_exception(exit)}
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_status(opts) do
    keyspace =
      Keyword.fetch!(opts, :keyspace) || raise ":keyspace is nil in repository configuration"

    opts = Keyword.delete(opts, :keyspace)

    check_database_query = "USE #{keyspace}"

    case run_query(check_database_query, opts) do
      {:ok, %Xandra.SetKeyspace{keyspace: ^keyspace}} -> :up
      {:error, %Xandra.Error{reason: :invalid}} -> :down
      {:error, other} -> {:error, other}
      other -> other
    end
  end

  @impl true
  def insert(adapter_meta, %{source: source, prefix: prefix, schema: schema}, params,
            {kind, conflict_params, _} = on_conflict, returning, opts) do
    {fields, values} = :lists.unzip(params)
    sql = @conn.insert(prefix, source, fields, [fields], on_conflict, returning, opts)
    values = prepare_values(schema, params)
    Ecto.Adapters.SQL.struct(adapter_meta, @conn, sql, :insert, source, [], values ++ conflict_params, kind, returning, opts)
  end

  @impl true
  def update(adapter_meta, %{source: source, prefix: prefix, schema: schema}, fields, params, returning, opts) do
    {_, field_values} = :lists.unzip(fields)
    sql = @conn.update(prefix, source, fields, params, returning)
    values = prepare_values(schema, fields ++ params)
    Ecto.Adapters.SQL.struct(adapter_meta, @conn, sql, :update, source, params, values, :raise, returning, opts)
  end

  @impl true
  def execute(adapter_meta, query_meta, query, params, opts) do
    Ecto.Adapters.SQL.execute(adapter_meta, query_meta, query, params, opts ++ [uuid_format: :binary])
  end

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

  ## Custom Cassandra types

  # def dumpers(:binary_id, type), do: [type]

  # def dumpers(datetime, type) when datetime in [:datetime, :utc_datetime, :naive_datetime] do
  #   IO.puts("Dumper called #{type}")
  #   [&to_dt/1]
  # end

  # def dumpers(:decimal, type) do
  #   IO.puts("Dumper called decimal #{type}")
  #   [&to_decimal/1]
  # end

  # def dumpers(_primitive, type) do
  #   IO.puts("Dumper called #{_primitive} #{type}")
  #   [type]
  # end

  # defp to_decimal(%Decimal{} = d), do: d
  # defp to_decimal(_), do: :error

  # defp to_dt(%NaiveDateTime{} = dt), do: DateTime.from_naive(dt, "Etc/UTC")
  # defp to_dt(%DateTime{} = dt), do: {:ok, dt}
  # defp to_dt(_), do: :error


  # @impl true
  # def loaders({:map, _}, type),   do: [&json_decode/1, &Ecto.Type.embedded_load(type, &1, :json)]
  # def loaders(:map, type),        do: [&json_decode/1, type]
  # def loaders(:float, type),      do: [&float_decode/1, type]
  # def loaders(:boolean, type),    do: [&bool_decode/1, type]
  # def loaders(:binary_id, type),  do: [Ecto.UUID, type]
  # def loaders(_, type),           do: [type]

  # defp bool_decode(<<0>>), do: {:ok, false}
  # defp bool_decode(<<1>>), do: {:ok, true}
  # defp bool_decode(<<0::size(1)>>), do: {:ok, false}
  # defp bool_decode(<<1::size(1)>>), do: {:ok, true}
  # defp bool_decode(0), do: {:ok, false}
  # defp bool_decode(1), do: {:ok, true}
  # defp bool_decode(x), do: {:ok, x}

  # defp float_decode(%Decimal{} = decimal), do: {:ok, Decimal.to_float(decimal)}
  # defp float_decode(x), do: {:ok, x}

  # defp json_decode(x) when is_binary(x), do: {:ok, MyXQL.json_library().decode!(x)}
  # defp json_decode(x), do: {:ok, x}

  # @callback dumpers(primitive_type :: Ecto.Type.primitive(), ecto_type :: Ecto.Type.t()) ::
  # [(term -> {:ok, term} | :error) | Ecto.Type.t()]
  # @impl true
  # def dumpers(k, v) do
  #   require IEx; IEx.pry 
  # end
  # def dumpers(:uuid, type), do: [type]

  # Migration
  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction? do
    false
  end

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_meta, _opts, fun) do
    {:ok, result} = fun.()
    result
  end

  ## Helpers

  defp run_query(sql, opts) do
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:xandra)

    opts =
      opts
      |> Keyword.drop([:name, :log, :pool, :pool_size])
      |> Keyword.put(:backoff_type, :stop)
      |> Keyword.put(:max_restarts, 0)

    task =
      Task.Supervisor.async_nolink(Ecto.Adapters.SQL.StorageSupervisor, fn ->
        {:ok, conn} = xandra_start(opts)

        value = Xandra.execute(conn, sql, [], opts)
        GenServer.stop(conn)
        value
      end)

    timeout = Keyword.get(opts, :timeout, 15_000)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, {:ok, result}} ->
        {:ok, result}

      {:ok, {:error, error}} ->
        {:error, error}

      {:exit, exit} ->
        {:exit, exit}

      nil ->
        {:error, RuntimeError.exception("command timed out")}
    end
  end

  defp xandra_start(opts) do
    case Keyword.get(opts, :nodes, nil) do
      nodes when is_list(nodes) and length(nodes) > 1 ->
        Xandra.Cluster.start_link(opts)

      _ ->
        Xandra.start_link(opts)
    end
  end

  defp exit_to_exception({%{__struct__: struct} = error, _})
       when struct in [Xandra.Error, DBConnection.Error],
       do: error

  defp exit_to_exception(reason), do: RuntimeError.exception(Exception.format_exit(reason))
end
