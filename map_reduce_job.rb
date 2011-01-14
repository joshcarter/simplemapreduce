#
# Copyright (c) 2006 Josh Carter <josh@multipart-mixed.com>
#
# All rights reserved.  You can redistribute and/or modify this under
# the same terms as Ruby.
#

require 'rinda/ring'
require 'worker_task'

# Users should create instances of this class. Rather than subclassing,
# jobs are specialized by assigning lambdas to map, reduce, and partition.
# This allows the instance to easily create sub-tasks and marshal the map
# and reduce code for sending to workers.
#
class MapReduceJob
  attr_accessor :data, :map_tasks, :reduce_tasks, :silent
  attr_accessor :map, :reduce, :partition # User-provided lambdas

  # New job: caller specifies quantity of map and reduce tasks.
  #
  def initialize(map_tasks = 10, reduce_tasks = 2)
    @map_tasks = map_tasks
    @reduce_tasks = reduce_tasks
    @silent = false
    
    # Start Rinda tuplespace
    #
    DRb.start_service
    ring_server = Rinda::RingFinger.primary

    ts = ring_server.read([:name, :TupleSpace, nil, nil])[2]
    @tuple_space = Rinda::TupleSpaceProxy.new ts
  end
  
  # Submit tasks to Rinda tuplespace, collect results. Used for
  # both map and reduce jobs.
  #
  def run_tasks(description, tasks)
    result = Array.new
    
    tasks.each do |task|
      puts "submitting #{description} task #{task.task_id}" unless @silent
      @tuple_space.write(['task', DRb.uri, task])
    end

    tasks.each_with_index do |task, i|
      # Get result: tuple[2] is task ID to look for, tuple[3] will be result
      tuple = @tuple_space.take(['result', DRb.uri, task.task_id, nil])
      result[i] = tuple[3]
    end
    
    result
  end
  
  # Run the job and return result arrays
  #
  def run
    unless (@map && @reduce && @partition)
      raise ArgumentError, "map and/or reduce lambdas not assigned"
    end

    # Partition up starting data, create map tasks
    #
    map_data = Partitioner::simple_partition_data(@data, @map_tasks)
    map_tasks = Array.new

    (0..@map_tasks - 1).each do |i|
      map_tasks << WorkerTask.new(i + 1, map_data[i], @map)
    end

    # Run map tasks
    #
    map_data = run_tasks("map", map_tasks)

    # Re-partition returning data for reduction, create reduce tasks
    #
    reduce_data = @partition.call(map_data, @reduce_tasks)
    reduce_tasks = Array.new
    
    (0..@reduce_tasks - 1).each do |i|
      reduce_tasks << WorkerTask.new(i + 1, reduce_data[i], @reduce)
    end

    # Reduce and return results
    #
    run_tasks("reduce", reduce_tasks)
  end
end

# Collection of partitioning utilities
#
module Partitioner
  
  # Split one block of data into partitions
  #
  def self.simple_partition_data(data, partitions)
    partitioned_data = Array.new

    # If data size is significantly greater than the number of desired
    # partitions, we can divide the data roughly but the last partition
    # may be smaller than the others.
    #
    if (data.length >= partitions * 2)
      # Use quicker but less "fair" method
      size = data.length / partitions

      if (data.length % partitions != 0)
        size += 1 # Last slice of leftovers
      end

      (0..partitions - 1).each do |i|
        partitioned_data[i] = data[i * size, size]
      end
    else
      # Slower method, but partitions evenly
      (0..partitions - 1).each { |i| partitioned_data[i] = Array.new }
    
      data.each_with_index do |datum, i|
        partitioned_data[i % partitions] << datum
      end
    end
    
    partitioned_data
  end
  
  # Simple/stupid combine data and re-split, only dubiously useful.
  #
  def self.recombine_and_split
    lambda do |partitioned_data, new_partitions|
      data = Array.new
    
      partitioned_data.each do |partition|
        partition.each do |item|
          data << item
        end
      end
    
      Partitioner::simple_partition_data(data, new_partitions)
    end
  end
  
  # Smarter partitioner for array data, generates simple sum of array[0]
  # and ensures that all arrays sharing that key go into the same partition.
  #
  def self.array_data_split_by_first_entry
    lambda do |partitioned_data, new_partitions|
      partitions = Array.new
      (0..new_partitions - 1).each { |i| partitions[i] = Array.new }

      partitioned_data.each do |partition|
        partition.each do |array|
          key = 0
          array[0].each_byte { |c| key += c }

          partitions[key % new_partitions] << array
        end
      end

      partitions
    end
  end
  
end