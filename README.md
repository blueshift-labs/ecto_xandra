# EctoCass

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ecto_xandra` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ecto_xandra, "~> 0.1.15"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/ecto_xandra](https://hexdocs.pm/ecto_xandra).

## Usage

Example 

```elixir
defmodule EctoXandra.Model do
  use Ecto.Schema

  alias __MODULE__
  alias Ecto.UUID
  alias EctoXandra.Types.{Set, Timestamp, Json, Map}

  import Ecto.Changeset

  @primary_key false
  schema "models" do
    field :id, :string, primary_key: true
    flied :uuid_set, Set, type: UUID
    flied :data_map, Map, key: :string, value: :integer
    field :attributes, Json, default: %{}, source: :json_attributes
    field :version, :string, default: "1"

    field :created_at, Timestamp, autogenerate: true
    field :updated_at, Timestamp
  end

```