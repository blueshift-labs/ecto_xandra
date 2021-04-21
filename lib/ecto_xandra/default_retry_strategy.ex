defmodule EctoXandra.DefaultRetryStrategy do
  @behaviour Xandra.RetryStrategy
  @retry_count 5
  @consistency :quorum

  @impl true
  def new(options) do
    Keyword.get(options, :retry_count, @retry_count)
  end

  @impl true
  def retry(_error, _options, _retries_left = 0), do: :error

  @impl true
  def retry(_error, options, retries_left) do
    {:retry, options, retries_left - 1}
  end
end
