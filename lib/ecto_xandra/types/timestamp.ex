defmodule EctoXandra.Types.Timestamp do
  @moduledoc """
  Truncates timestamp to milliseconds for cassandra
  """
  use Timex

  @behaviour Ecto.Type

  @impl true
  def type, do: :timestamp

  @impl true
  def cast(nil), do: {:ok, nil}

  def cast(input) when is_binary(input) do
    case Timex.parse(input, "{RFC3339}") do
      {:ok, %DateTime{} = t} -> {:ok, t}
      {:ok, %NaiveDateTime{} = t} -> {:ok, Timex.to_datetime(t)}
      {:error, _} -> :error
    end
  end

  def cast(%DateTime{} = t), do: {:ok, t}

  def cast(dt) do
    case Timex.to_datetime(dt) do
      {:error, _} -> :error
      dt -> cast(dt)
    end
  end

  @impl true
  def load(nil), do: {:ok, nil}
  def load(%DateTime{} = t), do: {:ok, t}
  def load(_), do: :error

  @impl true
  def dump(nil), do: {:ok, nil}
  def dump(%DateTime{} = t), do: {:ok, t}
  def dump(_), do: :error

  @impl true
  def autogenerate() do
    Timex.now() |> DateTime.truncate(:millisecond)
  end

  @impl true
  def equal?(nil, nil), do: true
  def equal?(_, nil), do: false
  def equal?(nil, _), do: false

  def equal?(%DateTime{} = t1, %DateTime{} = t2) do
    DateTime.compare(
      DateTime.truncate(t1, :millisecond),
      DateTime.truncate(t2, :millisecond)
    ) == :eq
  end

  def equal?(_, _), do: false

  @impl true
  def embed_as(_), do: :self
end
