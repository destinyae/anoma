defmodule Anoma.Node.Transaction.ShardSupervisor do
  @moduledoc """
  I am the supervisor for `Anoma.Node.Transaction.Shard` processes.

  I start and manage individual `Shard` processes according to a strategy and
  schema provided in my arguments. I create and maintain a named ETS table
  (`:shard_key_map`) mapping keys to the registered name
  (`{:via, Registry, {Anoma.Node, {Shard, key}}}`) of the `Shard` process
  responsible for that key. The actual lookup of keys is handled by the
  `Anoma.Node.Transaction.ShardRouter`.

  ### Key Concepts

  - **Supervisor Args:** Keyword list including `:strategy` and `:schema`.
  - **Strategy:** Determines how shards are created (e.g., `:one_per_key`).
  - **Schema:** Defines the initial keys and their starting values.
  - **ETS Table:** `:shard_key_map` for key -> shard name lookup (used by ShardRouter).

  ### Public API

  - `start_link/1`: I start the supervisor.
  """

  use Supervisor

  alias Anoma.Node.Registry
  alias Anoma.Node.Transaction.Shard
  alias Anoma.Node.Transaction.ShardRouter

  require Logger

  ############################################################
  #                       Types                              #
  ############################################################

  @typedoc "I represent a key managed by a shard."
  @type key_t :: binary()

  @typedoc "I represent the initial value associated with a key in a shard."
  @type initial_value_t :: any()

  @typedoc """
  I am the schema defining the keys and their initial values for shards.
  For `:one_per_key` strategy, I expect a list containing either `key` binaries
  or `{key, initial_value}` tuples. If only a key is provided, there is no
  initial value.
  """
  @type schema_t :: [key_t() | {key_t(), initial_value_t()}]

  @typedoc """
  I am the sharding strategy.
  Currently, I only support `:one_per_key`.
  """
  @type strategy_t :: :one_per_key

  @typedoc """
  I am the type of the arguments that the ShardSupervisor expects at startup.
  I require `:node_id` and optionally `:strategy` and `:schema` keys.
  """
  @type supervisor_args_t :: [
          node_id: String.t(),
          strategy: strategy_t() | nil,
          schema: schema_t() | nil
        ]

  @typedoc "I am the type of the arguments that the Shard process expects."
  @type shard_args_t :: [id: key_t(), initial_kv: %{key_t() => initial_value_t()}]

  ############################################################
  #                       Constants                          #
  ############################################################

  @ets_table_name :shard_key_map
  @registry_name Anoma.Node

  ############################################################
  #                 Supervisor Implementation                #
  ############################################################

  @doc """
  I am the start_link function for the ShardSupervisor.

  I start and link the supervisor process under the current supervision tree,
  registering myself locally using a node-specific name.
  """
  @spec start_link(args :: supervisor_args_t()) :: Supervisor.on_start()
  def start_link(args) do
    # Use node_id for registration
    node_id = Keyword.fetch!(args, :node_id)
    name = Registry.via(@registry_name, {__MODULE__, node_id})
    Supervisor.start_link(__MODULE__, args, name: name)
  end

  @impl true
  @doc """
  I am the Supervisor initialization callback.

  I set the process label. If valid :strategy and :schema are provided,
  I calculate the full key->name mapping and shard child specs,
  populate the `:shard_key_map` ETS table, and then start the
  `ShardRouter` and all configured `Shard` children using a
  `:one_for_one` strategy.
  """
  @spec init(args :: supervisor_args_t()) :: {:ok, {Supervisor.sup_flags(), [Supervisor.child_spec()]}}
  def init(args) do
    node_id = Keyword.fetch!(args, :node_id)
    Process.set_label({__MODULE__, node_id})
    Logger.info("Initializing ShardSupervisor #{node_id} with args: #{inspect(args)}")

    # Process schema only if strategy and schema are validly provided
    {shard_child_specs, key_to_name_map} =
      case {Keyword.get(args, :strategy), Keyword.get(args, :schema)} do
        {:one_per_key, schema} when is_list(schema) ->
          Logger.info("Building shard specs and map for ShardSupervisor #{node_id}...")

          # Iterate schema once to build specs and key->name map
          Enum.reduce(schema, {[], %{}}, fn schema_entry, {specs_acc, map_acc} ->
            case schema_entry do
              # Case 1: Schema entry is {key, initial_value}
              {key, initial_value} when is_binary(key) ->
                shard_id = key
                shard_name = Registry.via(node_id, Shard, shard_id)
                shard_args = [node_id: node_id, id: shard_id, initial_kv: %{key => initial_value}]
                child_spec = %{id: shard_id, start: {Shard, :start_link, [shard_args]}}
                {[child_spec | specs_acc], Map.put(map_acc, key, shard_name)}

              # Case 2: Schema entry is just a key
              key when is_binary(key) ->
                shard_id = key
                shard_name = Registry.via(node_id, Shard, shard_id)
                shard_args = [node_id: node_id, id: shard_id, initial_kv: %{}]
                child_spec = %{id: shard_id, start: {Shard, :start_link, [shard_args]}}
                {[child_spec | specs_acc], Map.put(map_acc, key, shard_name)}

              invalid_entry ->
                 Logger.error("Invalid schema entry for :one_per_key strategy (Node: #{node_id}): #{inspect(invalid_entry)}. Skipping.")
                 {specs_acc, map_acc} # Skip invalid entry
            end
          end)
          |> then(fn {specs, map} -> {Enum.reverse(specs), map} end) # Reverse specs for order

        {nil, _} ->
          Logger.info("No :strategy provided for ShardSupervisor #{node_id}, starting no shards.")
          {[], %{}}

        {_strategy, nil} ->
           Logger.info("No :schema provided for ShardSupervisor #{node_id}, starting no shards.")
           {[], %{}}

         {invalid_strategy, _} ->
           Logger.error("Unsupported shard strategy (Node: #{node_id}): #{inspect(invalid_strategy)}")
           {[], %{}}
      end

    # Create and populate ETS table if shards were generated
    if map_size(key_to_name_map) > 0 do
      Logger.debug("Populating ETS table :shard_key_map for node #{node_id}")
      ets_table = :ets.new(@ets_table_name, [:set, :public, :named_table, read_concurrency: true])

      # Verify table creation/access and log insertions
      if :ets.info(ets_table, :name) == @ets_table_name do
        Logger.debug("ETS table :shard_key_map verified/created for node #{node_id}. Owner: #{inspect(:ets.info(ets_table, :owner))}")
        for {key, name} <- key_to_name_map do
          Logger.debug("ShardSupervisor #{node_id}: Inserting into ETS: Key=#{inspect(key)}, Name=#{inspect(name)}")
          :ets.insert(ets_table, {key, name})
        end
      else
        Logger.error("ShardSupervisor #{node_id}: Failed to create or verify ETS table :shard_key_map")
      end

      # Define router child spec (only needed if shards exist)
      router_child_spec = {ShardRouter, [node_id: node_id]}
      all_children = [router_child_spec | shard_child_specs]

      Logger.debug("ShardSupervisor #{node_id} starting children: #{inspect(all_children)}")
      Supervisor.init(all_children, strategy: :one_for_one)
    else
      # No shards configured or generated, start no children
      Logger.debug("ShardSupervisor #{node_id} starting no children.")
      Supervisor.init([], strategy: :one_for_one)
    end
  end

  ############################################################
  #                       Public API                         #
  ############################################################

  ############################################################
  #                    Private Helpers                       #
  ############################################################

end
