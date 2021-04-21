defmodule EctoXandra.Types.Json do
  @behaviour Ecto.Type

  @impl true
  def type, do: :text

  @impl true
  def cast(%{} = map), do: {:ok, map}
  def cast(list) when is_list(list), do: {:ok, list}
  def cast(_other), do: :error

  @impl true
  def load(%{} = map), do: {:ok, map}
  def load(list) when is_list(list), do: {:ok, list}

  def load(string) when is_binary(string) do
    case Jason.decode(string) do
      {:ok, data} -> {:ok, data}
      {:error, _} -> :error
    end
  end

  @impl true
  def dump(data) do
    case Jason.encode(data) do
      {:ok, string} -> {:ok, string}
      {:error, _} -> :error
    end
  end

  @impl true
  def equal?(%{} = a, %{} = b), do: Map.equal?(a, b)
  def equal?(a, b) when is_list(a) and is_list(b), do: a == b
  def equal?(_, _), do: false

  @impl true
  def embed_as(_), do: :self
end
