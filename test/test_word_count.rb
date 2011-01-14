#
# Copyright (c) 2006 Josh Carter <josh@multipart-mixed.com>
#
# All rights reserved.  You can redistribute and/or modify this under
# the same terms as Ruby.
#

require 'test/unit'
require 'map_reduce_job'

# Simple word count test; see MapReduce paper section 2.3
# http://labs.google.com/papers/mapreduce.html
#
class WordCountTest < Test::Unit::TestCase
  
  def test_word_count
    job = setup_job
    result = job.run
    
    # Recombine reduced data into single index
    counts = Hash.new
    result.each do |partition|
      partition.each_pair do |key, value|
        counts[key] = value
      end
    end

    assert_equal 8, counts['dog']
    assert_equal 7, counts['cat']
    assert_equal 6, counts['bird']
    assert_equal 5, counts['horse']
    assert_equal 4, counts['donkey']
    assert_equal 3, counts['shrew']
    assert_equal 2, counts['elephant']
    assert_equal 1, counts['platypus']
  end

  def setup_job
    job = MapReduceJob.new(4, 3)
    job.silent = true
    job.partition = Partitioner::array_data_split_by_first_entry
    
    # Data: simple strings of words, pre-split into arrays for map jobs.
    #
    job.data = [
      'dog cat bird horse donkey shrew elephant platypus',
      'dog cat bird horse donkey shrew elephant',
      'dog cat bird horse donkey shrew',
      'dog cat bird horse donkey',
      'dog cat bird horse',
      'dog cat bird',
      'dog cat',
      'dog']

    # Map: take string, return one pair of [word, 1] for each word.
    #
    job.map = lambda do |lines|
      result = Array.new

      lines.each do |line|
        next if line.empty?

        line.scan(/\w+/).each do |word|
          result << [ word, 1 ]
        end
      end

      result
    end

    # Reduce: Combine [word, 1] pairs into [word, count]. The partitioner
    # will ensure that for each word, all instances of that word will wind
    # up at the same worker.
    #
    job.reduce = lambda do |pairs|
      counts = Hash.new

      pairs.each do |pair|
        word, count = pair[0], pair[1]

        counts[word] ||= 0
        counts[word] += count
      end

      counts
    end
    
    job
  end

end
