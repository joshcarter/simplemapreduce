#
# Copyright (c) 2006 Josh Carter <josh@multipart-mixed.com>
#
# All rights reserved.  You can redistribute and/or modify this under
# the same terms as Ruby.
#

require 'test/unit'
require 'map_reduce_job'
require 'pp'

# Tests for the partitioners.
#
class DataPartitionTest < Test::Unit::TestCase

  def sample_data(size)
    data = Array.new
    (1..size).each do |i|
      data << i
    end
    
    data
  end
  
  def test_simple_partition_data
    data = sample_data(100)

    # Should evenly split into 5 20-item partitions
    partitioned_data = Partitioner::simple_partition_data(data, 5)
    assert_equal 20, partitioned_data[0].length
    assert_equal 1, partitioned_data[0][0]
    assert_equal 20, partitioned_data[0][19]
    assert_equal 20, partitioned_data[4].length
    assert_equal 81, partitioned_data[4][0]
    assert_equal 100, partitioned_data[4][19]

    # Should split into 6 16-item partitions and one leftover of 10
    partitioned_data = Partitioner::simple_partition_data(data, 7)
    assert_equal 15, partitioned_data[0].length
    assert_equal 15, partitioned_data[5].length
    assert_equal 10, partitioned_data[6].length

    # Should split into 1 partition
    partitioned_data = Partitioner::simple_partition_data(data, 1)
    assert_equal 1, partitioned_data.length
    assert_equal 100, partitioned_data[0].length

    # Split into 6 partitions, uses smarter but slower partitioner
    data = sample_data(8)    
    partitioned_data = Partitioner::simple_partition_data(data, 6)
    assert_equal 2, partitioned_data[0].length
    assert_equal 2, partitioned_data[1].length
    assert_equal 1, partitioned_data[2].length
    assert_equal 1, partitioned_data[3].length
    assert_equal 1, partitioned_data[4].length
    assert_equal 1, partitioned_data[5].length
  end
  
  def test_default_reduce
    data = sample_data(100)

    # Evenly split into 5 20-item partitions
    partitioned_data = Partitioner::simple_partition_data(data, 5)
    
    # Re-combine into 20 5-item partitions
    partitioned_data = Partitioner::recombine_and_split.call(partitioned_data, 20)
    assert_equal 20, partitioned_data.length
    assert_equal 5, partitioned_data[0].length
    assert_equal 1, partitioned_data[0][0]
    assert_equal 100, partitioned_data[19][4]
  end
end
