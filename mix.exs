defmodule EctoXandra.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_xandra,
      version: "0.1.20",
      elixir: "~> 1.14",
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
      {:ecto, "~> 3.9"},
      {:ecto_sql, "~> 3.9"},
      {:nimble_lz4, "~> 0.1.2"},
      {:xandra, git: "https://github.com/blueshift-labs/xandra.git", tag: "v0.14.0-bsft-17"},
      {:jason, "~> 1.2"}
    ]
  end
end
