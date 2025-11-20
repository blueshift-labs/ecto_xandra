defmodule EctoXandra.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_xandra,
      version: "0.1.20",
      elixir: "~> 1.15.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:timex, "~> 3.7"},
      {:ecto, "~> 3.11"},
      {:ecto_sql, "~> 3.11"},
      {:nimble_lz4, "~> 0.1.2", optional: true},
      {:xandra,
       git: "https://github.com/blueshift-labs/xandra.git",
       ref: "9a3cd6e944b19d428192d531c7bb4ec7e498be63"},
      {:jason, "~> 1.2"}
    ]
  end
end
