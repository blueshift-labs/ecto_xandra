defmodule EctoXandra.Types.Double do
  @behaviour Ecto.Type

  @impl true
  def type, do: :double

  @impl true
  def cast(nil), do: {:ok, nil}
  def cast(input) when is_float(input), do: {:ok, input}
  def cast(_input), do: :error

  @impl true
  def load(nil), do: {:ok, nil}
  def load(d) when is_float(d), do: {:ok, d}
  def load(_), do: :error

  @impl true
  def dump(nil), do: {:ok, nil}
  def dump(d) when is_float(d), do: {:ok, d}
  def dump(_), do: :error

  @impl true
  def equal?(d1, d2), do: d1 == d2

  @impl true
  def embed_as(_), do: :self
end
