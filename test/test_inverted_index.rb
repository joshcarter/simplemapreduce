#
# Copyright (c) 2006 Josh Carter <josh@multipart-mixed.com>
#
# All rights reserved.  You can redistribute and/or modify this under
# the same terms as Ruby.
#

require 'test/unit'
require 'map_reduce_job'

# Simple inverted index test; see MapReduce paper section 2.3
# http://labs.google.com/papers/mapreduce.html
#
class InvertedIndexTest < Test::Unit::TestCase
  
  def test_inverted_index
    job = setup_job
    result = job.run

    # Recombine reduced data into single index
    index = Hash.new
    result.each do |partition|
      partition.each_pair do |key, value|
        index[key] = value
      end
    end
  
    assert_equal ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'], index['dog'].sort
    assert_equal ['A', 'B', 'C', 'D', 'E', 'F', 'G'], index['cat'].sort
    assert_equal ['A', 'B', 'C', 'D', 'E', 'F'], index['bird'].sort
    assert_equal ['A', 'B', 'C', 'D', 'E'], index['horse'].sort
    assert_equal ['A', 'B', 'C', 'D'], index['donkey'].sort
    assert_equal ['A', 'B', 'C'], index['shrew'].sort
    assert_equal ['A', 'B'], index['elephant'].sort
    assert_equal ['A'], index['platypus'].sort
  end

  def setup_job
    job = MapReduceJob.new(4, 2)
    job.silent = true
    job.partition = Partitioner::array_data_split_by_first_entry
    
    # Data: simulate documents (A..H) containing words
    #
    job.data = Array.new
    job.data << ['A', 'dog cat bird horse donkey shrew elephant platypus']
    job.data << ['B', 'dog cat bird horse donkey shrew elephant']
    job.data << ['C', 'dog cat bird horse donkey shrew']
    job.data << ['D', 'dog cat bird horse donkey']
    job.data << ['E', 'dog cat bird horse']
    job.data << ['F', 'dog cat bird']
    job.data << ['G', 'dog cat']
    job.data << ['H', 'dog']
    
    # Map: for each document, take the words contained and create pairs of
    # [word, document name].
    #
    job.map = lambda do |documents|
      result = Array.new

      documents.each do |document|
        name = document[0]
        words = document[1]
        next if words.empty?

        words.scan(/\w+/).each do |word|
          result << [ word, name ]
        end
      end

      result
    end

    # Reduce: combine [word, document name] pairs, creating one pair per
    # word of [word, all containing document names]. All pairs for any
    # given word will wind up in the same worker because the partitioner
    # guarantees it.
    #
    job.reduce = lambda do |pairs|
      index = Hash.new

      pairs.each do |pair|
        word, document  = pair[0], pair[1]

        index[word] ||= Array.new
        index[word] << document
      end

      index
    end
    
    job
  end

end
