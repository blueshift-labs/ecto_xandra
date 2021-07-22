defmodule EctoXandra.Types do
  def apply(type, op, value, opts) do
    cond do
      is_atom(type) and function_exported?(type, op, 1) ->
        apply(type, op, [value])

      is_atom(type) and function_exported?(type, op, 2) ->
        apply(type, op, [value, opts])

      true ->
        Ecto.Type.cast(type, value)
    end
  end
end
