defmodule EctoXandra.LZ4Compressor do
  @behaviour Xandra.Compressor

  @impl true
  def algorithm(), do: :lz4

  @impl true
  def compress(body) do
    [<<IO.iodata_length(body)::4-unit(8)-integer>>, NimbleLZ4.compress(body)]
  end

  @dialyzer {:nowarn_function, decompress: 1}

  @impl true
  def decompress(<<uncompressed_size::4-unit(8)-integer, compressed_body::binary>>) do
    {:ok, body} = NimbleLZ4.decompress(compressed_body, uncompressed_size)
    body
  end
end
