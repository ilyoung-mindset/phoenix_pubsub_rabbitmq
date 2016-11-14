defmodule Phoenix.PubSub.RabbitMQ do
  use Supervisor
  use AMQP
  require Logger

  @pool_size 5

  @moduledoc """
  The Supervisor for the RabbitMQ `Phoenix.PubSub` adapter

  To use RabbitMQ as your PubSub adapter, simply add it to your Endpoint's config:

      config :my_app, MyApp.Endpoint,
        ...
        pubsub: [adapter: Phoenix.PubSub.RabbitMQ,
                 options: [host: "localhost"]


  next, add `:phoenix_rabbitmq_pubsub` to your deps:

      defp deps do
        [{:amqp, "~> 0.1.0"},
         {:poolboy, "~> 1.4.2"},
        ...]
      end

  finally, add `:phoenix_rabbitmq_pubsub` to your applications:

      def application do
        [mod: {MyApp, []},
         applications: [..., :phoenix, :phoenix_rabbitmq_pubsub],
         ...]
      end

    * `name` - The required name to register the PubSub processes, ie: `MyApp.PubSub`
    * `options` - The optional RabbitMQ options:
      * `host` - The hostname of the broker (defaults to \"localhost\");
      * `port` - The port the broker is listening on (defaults to `5672`);
      * `username` - The name of a user registered with the broker (defaults to \"guest\");
      * `password` - The password of user (defaults to \"guest\");
      * `virtual_host` - The name of a virtual host in the broker (defaults to \"/\");
      * `heartbeat` - The hearbeat interval in seconds (defaults to `0` - turned off);
      * `connection_timeout` - The connection timeout in milliseconds (defaults to `infinity`);
      * `pool_size` - Number of active connections to the broker

  """

  def start_link(name, opts \\ []) do
    supervisor_name = Module.concat(__MODULE__, name)
    Supervisor.start_link(__MODULE__, [name, opts], name: supervisor_name)
  end

  def init([name, opts]) do
    conn_pool_base = Module.concat(__MODULE__, ConnPool)
    pub_pool_base  = Module.concat(__MODULE__, PubPool)
    bk_conn_pool_base = Module.concat(__MODULE__, BkConnPool)

    options = opts[:options] || []
    hosts = options[:hosts] || ["localhost"]
    shard_num = length(hosts)
    HashRing.Managed.new(:rabbitmq_pubsub_shard)
    HashRing.Managed.add_nodes(:rabbitmq_pubsub_shard, hosts)

    bk_hosts = options[:bk_hosts] || []
    bk_shard_num = length(bk_hosts)
    HashRing.Managed.new(:rabbitmq_pubsub_bk_shard)
    HashRing.Managed.add_nodes(:rabbitmq_pubsub_bk_shard, bk_hosts)

    # to make state smaller
    options = List.keydelete(options, :hosts, 0) |> List.keydelete(:bk_hosts, 0)

    conn_pools = hosts |> Enum.map(fn(host) ->
      conn_pool_name = create_pool_name(conn_pool_base, host)
      conn_pool_opts = [
        name: {:local, conn_pool_name},
        worker_module: Phoenix.PubSub.RabbitMQConn,
        size: opts[:pool_size] || @pool_size,
        strategy: :fifo,
        max_overflow: 0
      ]
      :poolboy.child_spec(conn_pool_name, conn_pool_opts, [options ++ [host: host]])
    end)
    bk_conn_pools = bk_hosts |> Enum.map(fn(host) ->
      conn_pool_name = create_pool_name(bk_conn_pool_base, host)
      conn_pool_opts = [
        name: {:local, conn_pool_name},
        worker_module: Phoenix.PubSub.RabbitMQConn,
        size: opts[:pool_size] || @pool_size,
        strategy: :fifo,
        max_overflow: 0
      ]
      :poolboy.child_spec(conn_pool_name, conn_pool_opts, [options ++ [host: host]])
    end)

    pub_pools = hosts |> Enum.map(fn(host) ->
      conn_pool_name  = create_pool_name(conn_pool_base, host)
      pub_pool_name   = create_pool_name(pub_pool_base, host)
      pub_pool_opts = [
        name: {:local, pub_pool_name},
        worker_module: Phoenix.PubSub.RabbitMQPub,
        size: opts[:pool_size] || @pool_size,
        max_overflow: 0
      ]
      :poolboy.child_spec(pub_pool_name, pub_pool_opts, conn_pool_name)
    end)

    pool_size = @pool_size
    dispatch_rules = [
        {:broadcast, Phoenix.PubSub.RabbitMQServer, [name]},
        {:subscribe, Phoenix.PubSub.RabbitMQServer, [name]},
        {:unsubscribe, Phoenix.PubSub.RabbitMQServer, [name]},
      ]

    children = conn_pools ++ pub_pools ++ [
      supervisor(Phoenix.PubSub.LocalSupervisor, [name, pool_size, dispatch_rules]),
      worker(Phoenix.PubSub.RabbitMQServer, [name, conn_pool_base, pub_pool_base, bk_conn_pool_base, options ++ [shard_num: shard_num, bk_shard_num: bk_shard_num]])
    ] ++ bk_conn_pools
    supervise children, strategy: :one_for_one
  end

  def target_shard_host(topic) do
    HashRing.Managed.key_to_node(:rabbitmq_pubsub_shard, topic)
  end

  def target_bk_shard_host(topic) do
    HashRing.Managed.key_to_node(:rabbitmq_pubsub_bk_shard, topic)
  end

  def create_pool_name(pool_base, host) do
    Module.concat([pool_base, "_#{host}"])
  end

  def with_conn(pool_name, fun) when is_function(fun, 1) do
    case get_conn(pool_name, 0, @pool_size) do
      {:ok, conn}      -> fun.(conn)
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_conn(pool_name, retry_count, max_retry_count) do
    case :poolboy.transaction(pool_name, &GenServer.call(&1, :conn)) do
      {:ok, conn}      -> {:ok, conn}
      {:error, _reason} when retry_count < max_retry_count ->
        get_conn(pool_name, retry_count + 1, max_retry_count)
      {:error, reason} -> {:error, reason}
    end
  end

  def publish(pool_name, exchange, routing_key, payload, options \\ []) do
    case get_chan(pool_name, 0, @pool_size) do
      {:ok, chan}      -> Basic.publish(chan, exchange, routing_key, payload,options)
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_chan(pool_name, retry_count, max_retry_count) do
    case :poolboy.transaction(pool_name, &GenServer.call(&1, :chan)) do
      {:ok, chan}      -> {:ok, chan}
      {:error, _reason} when retry_count < max_retry_count ->
        get_chan(pool_name, retry_count + 1, max_retry_count)
      {:error, reason} -> {:error, reason}
    end
  end

end
