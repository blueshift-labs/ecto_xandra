defmodule EctoXandra.Types.Set do
    use Ecto.ParameterizedType
  
    @impl true
    def type(%{type: type}), do: "set<#{EctoXandra.xandra_type(type)}>"
  
    @impl true
    def init(opts) do
      Enum.into(opts, %{})
    end
  
    @impl true
    def cast(list, %{type: type} = opts) when is_list(list) do
      casted =
        Enum.reduce_while(list, [], fn elem, acc ->
          cond do
            is_atom(type) and function_exported?(type, :cast, 1) ->
              case type.cast(elem) do
                {:ok, casted} ->
                  {:cont, [casted | acc]}
  
                {:error, reason} ->
                  {:halt, {:error, reason}}
  
                :error ->
                  {:halt, :error}
              end
  
            is_atom(type) and function_exported?(type, :cast, 2) ->
              case type.cast(elem, opts) do
                {:ok, casted} ->
                  {:cont, [casted | acc]}
  
                {:error, reason} ->
                  {:halt, {:error, reason}}
  
                :error ->
                  {:halt, :error}
              end
  
            true ->
              {:cont, [elem | acc]}
          end
        end)
  
      cond do
        is_list(casted) -> {:ok, MapSet.new(casted)}
        true -> casted
      end
    end
  
    def cast(%MapSet{} = mapset, opts) do
      mapset |> MapSet.to_list() |> cast(opts)
    end
  
    def cast({op, val}, opts) when op in [:add, :remove] do
      case cast(val, opts) do
        {:ok, casted} -> {:ok, {op, casted}}
        other -> other
      end
    end
  
    def cast(_, _), do: :error

    @impl true
    def load(mapset, _loader, _params), do: {:ok, mapset}
  
    @impl true
    def dump({op, mapset}, _dumper, _params), do: {:ok, {op, mapset}}
    def dump(mapset, _dumper, _params), do: {:ok, mapset}
  
    @impl true
    def equal?(%MapSet{} = a, {_, %MapSet{}} = b), do: false
    def equal?({_, %MapSet{}} = a, %MapSet{} = b), do: false
    def equal?({_, %MapSet{}} = a, {_, %MapSet{}} = b), do: false
    def equal?(%MapSet{} = a, %MapSet{} = b), do: MapSet.equal?(a, b)
    def equal?(_, _), do: false
  
    def embed_as(_), do: :self
  
    # def source(%MapSet{} = set, _opts), do: set
    # def source({_op, %MapSet{} = set}, _opts), do: set
  end
  