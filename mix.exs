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
      {:xandra, git: "https://github.com/blueshift-labs/xandra.git", ref: "3fb588a333ae816edeb6bda7d10dc1bad7b6ac96"},
      {:jason, "~> 1.2"}
    ]
  end
end
