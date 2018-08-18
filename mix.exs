defmodule Exmdb.Mixfile do
  use Mix.Project

  def project do
    [app: :exmdb,
     version: "0.1.0",
     elixir: "~> 1.4-dev",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     dialyzer: [plt_add_deps: true],
     deps: deps()]
  end

  def application do
    [applications: [:logger]]
  end

  defp deps do
    [{:elmdb, git: "https://github.com/coderdan/elmdb.git"}]
  end
end
