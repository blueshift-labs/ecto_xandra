defmodule EctoXandra.Types.List do
  use Ecto.ParameterizedType

  @impl true
  def type(%{type: type}), do: "list<#{EctoXandra.xandra_type(type)}>"

  @impl true
  def init(opts) do
    Enum.into(opts, %{})
  end

  @impl true
  def cast(nil, _), do: {:ok, []}

  def cast({op, val}, opts) when op in [:add, :remove] do
    case cast(val, opts) do
      {:ok, casted} -> {:ok, {op, casted}}
      other -> other
    end
  end

  def cast(list, %{type: type} = opts) when is_list(list) do
    casted =
      Enum.reduce_while(list, [], fn elem, acc ->
        case EctoXandra.Types.apply(type, :cast, elem, opts) do
          {:ok, casted} -> {:cont, [casted | acc]}
          err -> {:halt, err}
        end
      end)

    if is_list(casted), do: {:ok, Enum.reverse(casted)}, else: casted
  end

  def cast(val, %{split: true} = opts) when is_binary(val) do
    splitter = Map.get(opts, :splitter, ~r|\s*,\s*|)
    opts = [trim: Map.get(opts, :trim, true)]
    {:ok, String.split(String.trim(val), splitter, opts)}
  end

  def cast(val, %{type: type} = opts) do
    case EctoXandra.Types.apply(type, :cast, val, opts) do
      {:ok, casted} -> {:ok, [casted]}
      err -> err
    end
  end

  def cast(_, _), do: :error

  @impl true
  def load(list, _loader, %{type: type} = opts) when is_list(list) do
    loaded =
      Enum.reduce_while(list, [], fn elem, acc ->
        case EctoXandra.Types.apply(type, :load, elem, opts) do
          {:ok, loaded} -> {:cont, [loaded | acc]}
          err -> {:halt, err}
        end
      end)

    if is_list(loaded), do: {:ok, Enum.reverse(loaded)}, else: :error
  end

  def load(nil, _, _), do: {:ok, []}

  def load(_, _, _), do: :error

  @impl true
  def dump(data, _dumper, _opts), do: {:ok, data}

  @impl true
  def equal?(a, b, _) when is_list(a) and is_list(b), do: a == b
  def equal?(_, _, _), do: false

  def embed_as(_), do: :self
end
