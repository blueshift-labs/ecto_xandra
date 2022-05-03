defmodule EctoXandra.Types.Json do
  use Ecto.ParameterizedType

  @impl true
  def type(_), do: :text

  @impl true
  def init(opts) do
    Enum.into(opts, %{})
  end

  @impl true
  def cast(nil, %{default: default} = opts), do: cast(default, opts)
  def cast(%{} = map, _), do: {:ok, map}
  def cast(list, _) when is_list(list), do: {:ok, list}
  def cast(_other, _), do: :error

  @impl true
  def load(nil, loader, %{default: default} = opts), do: load(default, loader, opts)
  def load(%{} = map, _, _), do: {:ok, map}
  def load(list, _, _) when is_list(list), do: {:ok, list}

  def load(string, _, %{default: default}) when is_binary(string) do
    case Jason.decode(string) do
      {:ok, nil} -> {:ok, default}
      {:ok, data} -> {:ok, data}
      {:error, _} -> :error
    end
  end

  def load(nil, _, _), do: {:ok, nil}

  @impl true
  def dump(nil, dumper, %{default: default} = opts), do: dump(default, dumper, opts)

  def dump(data, _, _) do
    case Jason.encode(data) do
      {:ok, string} -> {:ok, string}
      {:error, _} -> :error
    end
  end

  @impl true
  def equal?(nil, nil, _), do: true
  def equal?(%{} = a, %{} = b, _), do: Map.equal?(a, b)
  def equal?(a, b, _) when is_list(a) and is_list(b), do: a == b
  def equal?(_, _, _), do: false

  def embed_as(_), do: :self
end
