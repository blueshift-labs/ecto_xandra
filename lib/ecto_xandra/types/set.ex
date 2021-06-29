defmodule EctoXandra.Types.Set do
  use Ecto.ParameterizedType

  @impl true
  def type(%{type: type}), do: "set<#{EctoXandra.xandra_type(type)}>"

  @impl true
  def init(opts) do
    Enum.into(opts, %{})
  end

  @impl true
  def cast(list, %{type: type} = opts) when is_list(list) do
    casted =
      Enum.reduce_while(list, [], fn elem, acc ->
        case EctoXandra.Types.apply(type, :cast, elem, opts) do
          {:ok, casted} -> {:cont, [casted | acc]}
          err -> {:halt, err}
        end
      end)

    if is_list(casted), do: {:ok, MapSet.new(casted)}, else: casted
  end

  def cast(val, %{type: type} = opts) do
    case EctoXandra.Types.apply(type, :cast, val, opts) do
      {:ok, casted} -> {:ok, MapSet.new([casted])}
      err -> err
    end
  end

  def cast(%MapSet{} = mapset, opts) do
    mapset |> MapSet.to_list() |> cast(opts)
  end

  def cast({op, val}, opts) when op in [:add, :remove] do
    case cast(val, opts) do
      {:ok, casted} -> {:ok, {op, casted}}
      other -> other
    end
  end

  def cast(_, _), do: :error

  @impl true
  def load(%MapSet{} = mapset, _loader, %{type: type} = opts) do
    loaded =
      Enum.reduce_while(mapset, [], fn elem, acc ->
        case EctoXandra.Types.apply(type, :load, elem, opts) do
          {:ok, loaded} -> {:cont, [loaded | acc]}
          err -> {:halt, err}
        end
      end)

    if is_list(loaded), do: {:ok, MapSet.new(loaded)}, else: :error
  end

  def load(nil, _, _), do: {:ok, %MapSet{}}

  def load(_, _, _), do: :error

  @impl true
  def dump(mapset, _dumper, _opts), do: {:ok, mapset}

  @impl true
  def equal?(%MapSet{}, {_, %MapSet{}}, _opts), do: false
  def equal?({_, %MapSet{}}, %MapSet{}, _opts), do: false
  def equal?({_, %MapSet{}}, {_, %MapSet{}}, _opts), do: false
  def equal?(%MapSet{} = a, %MapSet{} = b, _opts), do: MapSet.equal?(a, b)
  def equal?(_, _, _), do: false

  def embed_as(_), do: :self
end
