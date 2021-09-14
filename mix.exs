defmodule EctoXandra.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_xandra,
      version: "0.1.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:timex, "~> 3.7"},
      {:ecto, "~> 3.6"},
      {:ecto_sql, "~> 3.6"},
      {:xandra, git: "https://github.com/blueshift-labs/xandra.git", tag: "v0.13.1.tmp"},
      {:jason, "~> 1.2"}

      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
