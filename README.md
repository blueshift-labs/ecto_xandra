# EctoXandra

## Description
An ecto adapter for cassandra/scylladb, based on [Xandra](https://github.com/lexhide/xandra). 

This adapter uses Xandra.Cluster for all the operations and connection pooling.

A couple of custom data types are introduced to work with Cassandra composite data types: `Json`, `Map`, `List`, `Set`, `Timestamp`.

`EctoXandra.DefaultRetryStrategy` is used if not otherwise specified.

## Installation

Add `ecto_xandra` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ecto_xandra, "~> 0.1.17"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/ecto_xandra](https://hexdocs.pm/ecto_xandra).

## Usage
Config

```elixir
config :test_app, TestApp.Repo,
  telemetry_prefix: [:repo],
  protocol_version: :v4,
  pool_size: 10,
  default_consistency: :quorum,
  retry_count: 5
  log_level: :debug

config :utx, ecto_repos: [TestApp.Repo]

```

Repo

```elixir
defmodule TestApp.Repo do
  use Ecto.Repo, otp_app: :test_app, adapter: EctoXandra
end
```

Schema
```elixir
defmodule TestApp.Model do
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

Setup & Migration

mix.exs

```elixir
...
  defp elixirc_paths(:test), do: ["test/support", "lib", "priv/repo"]
  defp elixirc_paths(:dev), do: ["lib", "priv/repo"]
  defp elixirc_paths(_), do: ["lib"]

  defp aliases() do
    [
      "ecto.setup": ["ecto.create", &TestApp.Repo.Migration.run/1]
    ]
  end
...
```

priv/repo/migrateion.exs

```elixir
defmodule TestApp.Repo.Migration do
  def run(_) do
    Application.ensure_all_started(:test_app)

    Application.get_env(:test_app, :ecto_repos)
    |> Enum.each(fn repo ->
      %{adapter: EctoXandra, pid: conn} = Ecto.Repo.Registry.lookup(repo)

      statements(repo)
      |> Enum.each(&EctoXandra.Connection.execute(conn, &1, [], []))
    end)
  end

  defp statements(TestApp.Repo) do
    keyspace = Application.get_env(:test_app, repo) |> Keyword.fetch!(:keyspace)

    [
      """
      CREATE KEYSPACE IF NOT EXISTS #{keyspace}
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
      AND durable_writes = true;
      """,
      """
      CREATE TABLE IF NOT EXISTS #{keyspace}.models (
          id text,
          uuid_set set<uuid>,
          data_map map<text, int>,
          attributes text,
          version text,
          created_at timestamp,
          updated_at timestamp,
          PRIMARY KEY ((id))
      );
      """
    ]
  end
end

```

