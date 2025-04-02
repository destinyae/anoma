defmodule Anoma.Node.Examples.EShard do
  @moduledoc """
  I contain examples on how to interact with the Shard module.
  """

  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Registry
  alias Anoma.Node.Transaction.Shard

  import ExUnit.Assertions

  @doc """
  I start a Shard with a predefined initial state and verify that
  reading the initial state (at height 0) returns the correct values.
  """
  @spec start_and_test_initial_state() :: ENode.t()
  def start_and_test_initial_state() do
    enode = ENode.start_node()
    shard_id = :test_shard_1
    shard_via = Registry.via(Anoma.Node, {Shard, shard_id})

    initial_kv = %{
      "a" => 5,
      "c" => 15
      # "b" is intentionally omitted
    }

    # Start the shard
    {:ok, shard_pid} = Shard.start_link(id: shard_id, initial_kv: initial_kv)

    # --- Simulate Watermark Advancement prior to acquiring locks ---
    send(shard_pid, {:write_watermark_advanced, "c", 5})

    # --- Acquire Locks First ---
    {:ok, %{read: read_ref_a}} = Shard.lock(shard_via, "a", 0, :read)
    {:ok, %{read: read_ref_b}} = Shard.lock(shard_via, "b", 5, :read)
    {:ok, %{read: read_ref_c}} = Shard.lock(shard_via, "c", 4, :read)

    # --- Simulate Watermark Advancement after acquiring locks ---
    # Use send/2 because Shard handles these via handle_info
    send(shard_pid, {:write_watermark_advanced, "a", 0})
    send(shard_pid, {:write_watermark_advanced, "b", 10})

    # --- Test Reads (Now that watermarks allow immediate resolution) ---

    # Test key "a"
    assert Shard.read(shard_via, "a", 0, read_ref_a) == {:ok, 5}

    # Test key "b" (should be absent)
    assert Shard.read(shard_via, "b", 5, read_ref_b) == :absent

    # Test key "c"
    assert Shard.read(shard_via, "c", 4, read_ref_c) == {:ok, 15}

    # Return the enode for potential further use
    enode
  end

  @doc """
  I test the scenario where a read is requested before the watermark allows,
  then the watermark advances, and the read completes.
  """
  @spec test_queued_read() :: ENode.t()
  def test_queued_read() do
    enode = ENode.start_node()
    shard_id = :test_shard_queued
    shard_via = Registry.via(Anoma.Node, {Shard, shard_id})

    initial_kv = %{
      "a" => 5
    }

    # Start the shard
    {:ok, shard_pid} = Shard.start_link(id: shard_id, initial_kv: initial_kv)

    key = "a"
    height = 7

    # 1. Acquire Lock
    {:ok, %{read: read_ref}} = Shard.lock(shard_via, key, height, :read)

    # 2. Start the read in a separate task (it will block)
    read_task = Task.async(fn ->
      Shard.read(shard_via, key, height, read_ref)
    end)

    # Give the task a tiny moment to start and make the call
    Process.sleep(50)

    # Check shard state (debug)
    # :sys.get_state(shard_pid) |> IO.inspect(label: "Shard state before WM update")

    # 3. Advance the watermark *after* the read call is blocked
    send(shard_pid, {:write_watermark_advanced, key, height + 1})

    # Check shard state (debug)
    # :sys.get_state(shard_pid) |> IO.inspect(label: "Shard state AFTER WM update")

    # 4. Await the result from the task (should unblock now)
    # Use a timeout to prevent hangs if something is wrong
    result = Task.await(read_task, 1000)

    # 5. Assert the result
    assert result == {:ok, 5}

    enode
  end

  @doc """
  I test a variation of queued read where a write lock is acquired and
  a write is performed *after* the read is queued but *before* the read resolves,
  affecting the read's outcome.
  """
  @spec test_queued_read_with_intermediate_write() :: ENode.t()
  def test_queued_read_with_intermediate_write() do
    enode = ENode.start_node()
    shard_id = :test_shard_queued_write
    shard_via = Registry.via(Anoma.Node, {Shard, shard_id})

    key = "a"
    initial_value = 5
    read_height = 7
    write_height = 5 # Height for the intermediate write
    write_value = 10 # Value for the intermediate write

    initial_kv = %{
      key => initial_value
    }

    # Start the shard
    {:ok, shard_pid} = Shard.start_link(id: shard_id, initial_kv: initial_kv)

    # 1. Acquire Read Lock for the future read
    {:ok, %{read: read_ref}} = Shard.lock(shard_via, key, read_height, :read)

    # 2. Start the read in a separate task (it will block)
    read_task = Task.async(fn ->
      Shard.read(shard_via, key, read_height, read_ref)
    end)

    # Give the task a moment to start and block on the read call
    Process.sleep(50)

    # 3. Acquire Write Lock for an intermediate height BEFORE watermark advances
    {:ok, %{write: write_ref_intermediate}} = Shard.lock(shard_via, key, write_height, :write)

    # 4. Advance the watermark AFTER the read call is blocked, enabling read resolution
    send(shard_pid, {:write_watermark_advanced, key, read_height + 1}) # WM >= read_height

    # 5. Perform the Write AFTER watermark advanced but potentially before read task resumes
    # This write should be visible to the resolving read at height 7.
    assert :ok == Shard.write(shard_via, key, write_value, write_height, write_ref_intermediate)

    # 6. Await the result from the read task (should unblock due to WM)
    # Use a timeout to prevent hangs
    result = Task.await(read_task, 1000)

    # 7. Assert the result
    # The read at height 7 should resolve to the latest write strictly below 7.
    # The write at height 5 (value 10) occurred before the read resolved.
    # The initial value at -1 is 5.
    # Therefore, the latest write < 7 is the one at height 5.
    assert result == {:ok, write_value}

    enode
  end

  @doc """
  I test the scenario where a read is requested, but the watermark never
  advances, causing the read to time out (within the Task.await).
  """
  @spec test_read_timeout() :: ENode.t()
  def test_read_timeout() do
    enode = ENode.start_node()
    shard_id = :test_shard_timeout
    shard_via = Registry.via(Anoma.Node, {Shard, shard_id})

    # Start the shard (initial state doesn't matter)
    {:ok, _shard_pid} = Shard.start_link(id: shard_id, initial_kv: %{})

    key = "a"
    height = 5

    # 1. Acquire Lock
    {:ok, %{read: read_ref}} = Shard.lock(shard_via, key, height, :read)

    # 2. Start the read in a separate task (it will block)
    read_task = Task.async(fn ->
      # Note: The GenServer.call within Shard.read uses :infinity,
      # so this task itself won't timeout internally. The timeout
      # comes from Task.await below.
      Shard.read(shard_via, key, height, read_ref)
    end)

    # 3. DO NOT advance the watermark

    # 4. Await the result with a short timeout
    # We expect this to exit with reason :timeout
    try do
      Task.await(read_task, 100) # 100ms timeout
      # If await succeeds, the test fails
      flunk("Task.await should have timed out and exited, but it returned.")
    catch
      :exit, reason ->
        # Explicitly check the exit reason tuple if needed,
        # but typically checking the first element is sufficient for timeouts.
        assert reason == :timeout or match?({:timeout, _}, reason)
    end

    # Ensure the task is shut down to avoid lingering processes
    if Process.alive?(read_task.pid), do: Task.shutdown(read_task, :brutal_kill)

    enode
  end

  @doc """
  I test a more complex scenario involving multiple writes, reads, and
  write watermark advancements.
  """
  @spec test_complex_write_and_read_scenario() :: ENode.t()
  def test_complex_write_and_read_scenario() do
    enode = ENode.start_node()
    shard_id = :test_shard_complex_writes
    shard_via = Registry.via(Anoma.Node, {Shard, shard_id})
    key = "a"

    initial_kv = %{
      key => 3
    }

    # Start the shard
    {:ok, shard_pid} = Shard.start_link(id: shard_id, initial_kv: initial_kv)

    # --- Acquire Write Locks ---
    {:ok, %{write: write_ref_5}} = Shard.lock(shard_via, key, 5, :write)
    {:ok, %{write: write_ref_6}} = Shard.lock(shard_via, key, 6, :write)
    {:ok, %{write: write_ref_10}} = Shard.lock(shard_via, key, 10, :write)

    # --- Acquire Read Locks ---
    {:ok, %{read: read_ref_0}} = Shard.lock(shard_via, key, 0, :read)
    {:ok, %{read: read_ref_4}} = Shard.lock(shard_via, key, 4, :read)
    {:ok, %{read: read_ref_5}} = Shard.lock(shard_via, key, 5, :read)
    {:ok, %{read: read_ref_6}} = Shard.lock(shard_via, key, 6, :read)
    {:ok, %{read: read_ref_7}} = Shard.lock(shard_via, key, 7, :read)
    {:ok, %{read: read_ref_9}} = Shard.lock(shard_via, key, 9, :read)
    {:ok, %{read: read_ref_10}} = Shard.lock(shard_via, key, 10, :read)
    {:ok, %{read: read_ref_11}} = Shard.lock(shard_via, key, 11, :read)

    # --- Perform Writes ---
    assert :ok == Shard.write(shard_via, key, 7, 5, write_ref_5)
    assert :ok == Shard.write(shard_via, key, 2, 6, write_ref_6)
    assert :ok == Shard.write(shard_via, key, 8, 10, write_ref_10)

    # --- Simulate Watermark Advancements ---
    # Reads 0, 4, 5 depend on initial state (implied WM >= 0)
    send(shard_pid, {:write_watermark_advanced, key, 0}) # Effectively done by init

    # Read 6 needs to see write at 5
    send(shard_pid, {:write_watermark_advanced, key, 6})

    # Reads 7, 9, 10 need to see write at 6
    send(shard_pid, {:write_watermark_advanced, key, 7}) # WM advances to max(current, new)

    # Read 11 needs to see write at 10
    send(shard_pid, {:write_watermark_advanced, key, 11})

    # --- Test Reads ---
    # Read height h resolves based on latest write < h, provided WM >= h
    assert Shard.read(shard_via, key, 0, read_ref_0) == {:ok, 3} # Before any writes
    assert Shard.read(shard_via, key, 4, read_ref_4) == {:ok, 3} # Before write@5
    assert Shard.read(shard_via, key, 5, read_ref_5) == {:ok, 3} # Before write@5
    assert Shard.read(shard_via, key, 6, read_ref_6) == {:ok, 7} # Sees write@5
    assert Shard.read(shard_via, key, 7, read_ref_7) == {:ok, 2} # Sees write@6
    assert Shard.read(shard_via, key, 9, read_ref_9) == {:ok, 2} # Sees write@6
    assert Shard.read(shard_via, key, 10, read_ref_10) == {:ok, 2} # Sees write@6
    assert Shard.read(shard_via, key, 11, read_ref_11) == {:ok, 8} # Sees write@10

    enode
  end

  @doc """
  I test the internal state changes related to Garbage Collection (GC)
  and the state of entries after locks are released, using `:sys.get_state`
  for introspection.
  """
  @spec test_gc_and_lock_release_state() :: ENode.t()
  def test_gc_and_lock_release_state() do
    enode = ENode.start_node()
    shard_id = :test_shard_gc_lock_release
    shard_via = Registry.via(Anoma.Node, {Shard, shard_id})
    key = "a"

    initial_kv = %{key => 3}

    # Start the shard
    {:ok, shard_pid} = Shard.start_link(id: shard_id, initial_kv: initial_kv)

    # --- Writes ---
    write_ops = %{
      9 => 5,
      15 => 12,
      30 => 16,
      32 => 8
    }

    Enum.each(write_ops, fn {h, v} ->
      {:ok, %{write: write_ref}} = Shard.lock(shard_via, key, h, :write)
      assert :ok == Shard.write(shard_via, key, v, h, write_ref)
    end)

    # --- Direct State Check (Post-Write) ---
    # Use :sys.get_state for internal inspection (testing only)
    state1 = :sys.get_state(shard_pid)
    kv1 = state1.kv[key]

    assert Map.get(kv1, -1).value == 3
    assert Map.get(kv1, 9).value == 5 and is_nil(Map.get(kv1, 9).write_lock_ref)
    assert Map.get(kv1, 15).value == 12 and is_nil(Map.get(kv1, 15).write_lock_ref)
    assert Map.get(kv1, 30).value == 16 and is_nil(Map.get(kv1, 30).write_lock_ref)
    assert Map.get(kv1, 32).value == 8 and is_nil(Map.get(kv1, 32).write_lock_ref)
    assert map_size(kv1) == 5 # -1, 9, 15, 30, 32

    # --- Read Lock ---
    {:ok, %{read: read_ref_17}} = Shard.lock(shard_via, key, 17, :read)

    # Verify lock presence in state
    state2 = :sys.get_state(shard_pid)
    kv2 = state2.kv[key]
    assert kv2[17].read_lock_ref == read_ref_17
    assert is_nil(kv2[17].value)
    assert map_size(kv2) == 6 # Added entry for height 17

    # --- Advance Read Watermark (GC Trigger) ---
    send(shard_pid, {:read_watermark_advanced, key, 33})
    # Allow time for message processing
    Process.sleep(50)

    # --- Direct State Check (Post-GC) ---
    state3 = :sys.get_state(shard_pid)
    kv3 = state3.kv[key]

    # Expected remaining heights:
    # - 15: Kept because it's needed for read lock at 17 (max_h < 17)
    # - 17: Kept because it holds the active read lock.
    # - 32: Kept because it's the latest entry <= the watermark 33.
    assert Map.has_key?(kv3, 15)
    assert Map.get(kv3, 15).value == 12 # Check value consistency
    assert Map.has_key?(kv3, 17)
    assert kv3[17].read_lock_ref == read_ref_17 # Lock still held
    assert Map.has_key?(kv3, 32)
    assert Map.get(kv3, 32).value == 8 # Check value consistency
    assert map_size(kv3) == 3
    # Ensure others are gone
    refute Map.has_key?(kv3, -1)
    refute Map.has_key?(kv3, 9)
    refute Map.has_key?(kv3, 30)

    # --- Read Operation (at 17) ---
    # Advance WRITE watermark so read can resolve
    send(shard_pid, {:write_watermark_advanced, key, 18})
    # Perform the read
    assert Shard.read(shard_via, key, 17, read_ref_17) == {:ok, 12}

    # --- Direct State Check (Post-Read) ---
    state4 = :sys.get_state(shard_pid)
    kv4 = state4.kv[key]
    assert Map.has_key?(kv4, 17) # Entry should still exist
    assert is_nil(kv4[17].read_lock_ref) # Lock should be released
    assert is_nil(kv4[17].value)
    assert is_nil(kv4[17].write_lock_ref)
    assert map_size(kv4) == 3 # Size remains same, just lock released

    # --- Advance Read Watermark Again (Clean up entry 17) ---
    send(shard_pid, {:read_watermark_advanced, key, 34})
    Process.sleep(50)

    # --- Direct State Check (Final) ---
    state5 = :sys.get_state(shard_pid)
    kv5 = state5.kv[key]

    # Expected remaining heights:
    # - 32: Kept because it's the latest entry <= the new watermark 34.
    # Entries 15 and 17 should now be GC'd.
    assert Map.has_key?(kv5, 32)
    assert Map.get(kv5, 32).value == 8
    assert map_size(kv5) == 1
    # Ensure others are gone
    refute Map.has_key?(kv5, 15)
    refute Map.has_key?(kv5, 17)

    enode
  end
end
