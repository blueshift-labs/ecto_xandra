defmodule EctoCassTest do
  use ExUnit.Case
  doctest EctoCass

  test "greets the world" do
    assert EctoCass.hello() == :world
  end
end
