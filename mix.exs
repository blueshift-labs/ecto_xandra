defmodule EctoXandra.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_xandra,
      version: "0.1.17",
      elixir: "~> 1.13.4",
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
      {:ecto, "~> 3.8"},
      {:ecto_sql, "~> 3.8"},
      {:xandra, git: "https://github.com/blueshift-labs/xandra.git", tag: "v0.13.3"},
      {:jason, "~> 1.2"}
    ]
  end
end
