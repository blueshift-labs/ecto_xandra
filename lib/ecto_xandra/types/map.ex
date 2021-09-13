defmodule EctoXandra.Types.Map do
  use Ecto.ParameterizedType

  @impl true
  def type(%{key: key_type, value: value_type}),
    do: "map<#{EctoXandra.xandra_type(key_type)}, #{EctoXandra.xandra_type(value_type)}>"

  @impl true
  def init(opts) do
    Enum.into(opts, %{})
  end

  @impl true
  def cast(nil, _), do: {:ok, %{}}

  def cast({op, %{} = map}, opts) when op in [:add, :remove] do
    case cast(map, opts) do
      {:ok, casted} -> {:ok, {op, casted}}
      other -> other
    end
  end

  def cast(%{} = map, %{key: key_type, value: value_type} = opts) do
    casted =
      Enum.reduce_while(map, %{}, fn {k, v}, acc ->
        with {:ok, casted_key} <- EctoXandra.Types.apply(key_type, :cast, k, opts),
             {:ok, casted_value} <- EctoXandra.Types.apply(value_type, :cast, v, opts) do
          {:cont, Map.put(acc, casted_key, casted_value)}
        else
          _ -> {:halt, :error}
        end
      end)

    if is_map(casted), do: {:ok, casted}, else: casted
  end

  def cast(_, _), do: :error

  @impl true
  def load(%{} = map, _loader, %{key: key_type, value: value_type} = opts) do
    loaded =
      Enum.reduce_while(map, %{}, fn {k, v}, acc ->
        with {:ok, loaded_key} <- EctoXandra.Types.apply(key_type, :load, k, opts),
             {:ok, loaded_value} <- EctoXandra.Types.apply(value_type, :load, v, opts) do
          {:cont, Map.put(acc, loaded_key, loaded_value)}
        else
          _ -> {:halt, :error}
        end
      end)

    if is_map(loaded), do: {:ok, loaded}, else: :error
  end

  def load(nil, _, _), do: {:ok, %{}}

  def load(_, _, _), do: :error

  @impl true
  def dump(map, _dumper, _opts), do: {:ok, map}

  @impl true
  def equal?({_, _}, _, _), do: false
  def equal?(_, {_, _}, _), do: false
  def equal?(nil, nil, _), do: true
  def equal?(nil, data, _), do: Enum.empty?(data)
  def equal?(data, nil, _), do: Enum.empty?(data)
  def equal?(%{} = a, %{} = b, _), do: Map.equal?(a, b)
  def equal?(_, _, _), do: false

  def embed_as(_), do: :self
end
