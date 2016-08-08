defmodule Phoenix.PubSub.RabbitMQServer do
  use GenServer
  use AMQP
  alias Phoenix.PubSub.RabbitMQ
  alias Phoenix.PubSub.RabbitMQConsumer, as: Consumer
  require Logger

  @prefetch_count 10

  @moduledoc """
  `Phoenix.PubSub` adapter for RabbitMQ

  See `Phoenix.PubSub.RabbitMQ` for details and configuration options.
  """

  def start_link(server_name, conn_pool_base, pub_pool_base, opts) do
    GenServer.start_link(__MODULE__, [server_name, conn_pool_base, pub_pool_base, opts], name: server_name)
  end

  @doc """
  Initializes the server.

  """
  def init([server_name, conn_pool_base, pub_pool_base, opts]) do
    Process.flag(:trap_exit, true)
    ## TODO: make state compact
    {:ok, %{cons: :ets.new(:rmq_cons, [:set, :private]),
            subs: :ets.new(:rmq_subs, [:set, :private]),
            conn_pool_base: conn_pool_base,
            pub_pool_base: pub_pool_base,
            exchange: rabbitmq_namespace(server_name),
            node_ref: :crypto.strong_rand_bytes(16),
            opts: opts}}
  end

  def subscribe(server_name, pid, topic, opts) do
    GenServer.call(server_name, {:subscribe, pid, topic, opts})
  end
  def unsubscribe(server_name, pid, topic) do
    GenServer.call(server_name, {:subscribe, pid, topic})
  end
  def broadcast(server_name,from_pid, topic, msg) do
    GenServer.call(server_name, {:broadcast, from_pid, topic, msg})
  end

  def handle_call({:subscribe, pid, topic, opts}, _from, state) do
    link = Keyword.get(opts, :link, false)

    subs_list = :ets.lookup(state.subs, topic)
    has_key = case subs_list do
                [] -> false
                [{^topic, pids}] -> Enum.find_value(pids, false, fn(x) -> elem(x, 0) == pid end)
              end

    unless has_key do
      pool_index      = RabbitMQ.target_shard_index(state.opts[:shard_num], topic)
      conn_pool_name = RabbitMQ.create_pool_name(state.conn_pool_base, pool_index)
      {:ok, consumer_pid} = Consumer.start(conn_pool_name,
                                           state.exchange, topic,
                                           pid,
                                           state.node_ref,
                                           link)
      Process.monitor(consumer_pid)

      if link, do: Process.link(pid)

      :ets.insert(state.cons, {consumer_pid, {topic, pid}})
      pids = case subs_list do
        []                -> []
        [{^topic, pids}]  -> pids
      end
      :ets.insert(state.subs, {topic, pids ++ [{pid, consumer_pid}]})

      {:reply, :ok, state}
    end
  end

  def handle_call({:unsubscribe, pid, topic}, _from, state) do
    case :ets.lookup(state.subs, topic) do
      [] ->
        {:reply, :ok, state}
      [{^topic, pids}] ->
        case Enum.find(pids, false, fn(x) -> elem(x, 0) == pid end) do
          nil ->
            {:reply, :ok, state}
          {^pid, consumer_pid} ->
            :ok = Consumer.stop(consumer_pid)
            delete_subscriber(state.subs, pid, topic)
            {:reply, :ok, state}
        end
    end
  end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:subscribers, topic}, _from, state) do
    case :ets.lookup(state.subs, topic) do
      []                -> {:reply, [], state}
      [{^topic, pids}]  -> {:reply, Enum.map(pids, fn(x) -> elem(x, 0) end), state}
    end
  end

  def handle_call({:broadcast, from_pid, topic, msg}, _from, state) do
    pool_index    = RabbitMQ.target_shard_index(state.opts[:shard_num], topic)
    pub_pool_name = RabbitMQ.create_pool_name(state.pub_pool_base, pool_index)
    case RabbitMQ.publish(pub_pool_name,
                          state.exchange,
                          topic,
                          :erlang.term_to_binary({state.node_ref, from_pid, msg}),
                          content_type: "application/x-erlang-binary") do
      :ok              -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid,  _reason}, state) do
    state =
      case :ets.lookup(state.cons, pid) do
        [] -> state
        [{^pid, {topic, sub_pid}}] ->
          :ets.delete(state.cons, pid)
          delete_subscriber(state.subs, sub_pid, topic)
          state
      end
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, _reason}, state) do
    # Ignore subscriber exiting; the Consumer will monitor it
    {:noreply, state}
  end

  defp delete_subscriber(subs, pid, topic) do
    case :ets.lookup(subs, topic) do
      []                ->
        subs
      [{^topic, pids}]  ->
        remain_pids = List.keydelete(pids, pid, 0)
        if length(remain_pids) > 0 do
          :ets.insert(subs, {topic, remain_pids})
        else
          :ets.delete(subs, topic)
        end
        subs
    end
  end

  defp rabbitmq_namespace(server_name) do
    case Atom.to_string(server_name) do
      "Elixir." <> name -> name
      name              -> name
    end
  end

end
