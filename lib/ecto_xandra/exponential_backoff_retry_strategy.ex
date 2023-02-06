defmodule EctoXandra.ExponentialBackoffRetryStrategy do
  @behaviour Xandra.RetryStrategy

  alias DBConnection.Backoff

  @retry_count 5

  @impl true
  def new(options) do
    retry_count = Keyword.get(options, :retry_count, @retry_count)
    %{retry_count: retry_count, backoff: Backoff.new(options)}
  end

  @impl true
  def retry(_error, _options, %{retry_count: 0}), do: :error

  @impl true
  def retry(_error, options, %{retry_count: retry_count, backoff: backoff}) do
    {sleep, backoff} = Backoff.backoff(backoff)
    Process.sleep(sleep)
    {:retry, options, %{retry_count: retry_count - 1, backoff: backoff}}
  end
end
