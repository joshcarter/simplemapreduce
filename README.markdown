Simple MapReduce in Ruby
========================

Here's a simple version of the MapReduce framework presented in the
now-famous [Google paper][1] by Dean and Ghemawat. My version of
MapReduce is *not* intended as a usable high-performance framework,
but rather as a learning tool. My goal is twofold: first, to learn to
write algorithms in distributed/parallel MapReduce style. Second, to
see how simply these concepts can be expressed in Ruby.

[1]: http://labs.google.com/papers/mapreduce.html

I use the Rinda framework to distribute tasks to remote workers. This
simplifies a great deal of the MapReduce grunt work. The map and
reduce code, along with data, is marshaled and sent over the network
transparently. Creating a MapReduce job is as easy as creating an
object, assigning lambdas for map and reduce, assigning data, then
telling it to run.


Background
----------

If you haven't read the MapReduce paper, [do so now][2]. The concept
is simple: many algorithms can be broken up for computation on
clusters by providing a pair of functions: map and reduce. Map takes
key/value pairs and emits intermediate pairs, which are then merged as
appropriate by a reduce function. (Yea, this makes no sense at all
without seeing some examples. Skip to page 2 in the paper and look at
the examples.)

[2]: http://labs.google.com/papers/mapreduce.html


Example
-------

One of the simpler MapReduce algorithms discussed by Dean and Ghemawat
is counting URL accesses. The input as a web server log file, the
output is a list of URLs and the number of times each was accessed.
I'll jump right into the code and cover the MapReduce mechanics
afterwards.

First, I load the data into an array of lines from the log file(s):

    job = MapReduceJob.new
    job.data = File::readlines('logfile.txt')

Next, the map operation parses each line for HTML and XML requests
(i.e, only pages, not images) and for each emits [url, 1] pairs:

    job.map = lambda do |lines|
      result = Array.new
  
      lines.each do |line|
        line =~ /GET (\S+.(html|xml)) HTTP/
        result << [$1, 1] if $1
      end
  
      result
    end

You'll notice that the input data are lines, not pairs as discussed by
Dean and Ghemawat. My framework doesn't restrict the input/output
types; any built-in Ruby type is fair game.

The reduce operation merges the counts for each URL into one total per
URL:

    job.reduce = lambda do |pairs|
      totals = Hash.new
  
      pairs.each do |pair|
        url, count = pair[0], pair[1]

        totals[url] ||= 0
        totals[url] += count
      end
  
      totals
    end

Finally, the result of running a job is an array of return values from
the reduce operation, in this case an array of hashes. We can print
the totals for each URL like so:

    result = job.run

    result.each do |partition|
      partition.each_pair do |url, total|
        printf "%3d: %s\n", total, url
      end
    end


Quick Start
-----------

**[Download the code][3]** if you want to follow along.

[3]: http://multipart-mixed.com/downloads/software/simple_map_reduce.zip

Open a couple shell windows. Run "rake rinda" in one; this starts
background tasks for the ring server and tuplespace. Then run "rake
worker" in a couple. The workers can be spread over multiple machines,
just make sure they're running the same version of Ruby. Finally, run
"rake test" to run some jobs. You should see unit test results like "4
tests, 37 assertions, 0 failures, 0 errors."


Distributing Tasks with Rinda
-----------------------------

Creating a simple MapReduce is surprisingly easy in Ruby. The hardest
part is figuring out Rinda, as its English-language documentation is
sparse. Hopefully my code can give you a jump-start if you're
interested in it.

There are a couple of scripts you need to run first which set up the
Rinda tuplespace. I won't cover the code here as it's copy-n-paste
from the Rinda docs. (See ringserver.rb and tuplespace.rb.)

The principle is that Rinda provides a network space that any process
can write tuples (arrays of "stuff") to, and any other process can
take tuples from it. In my version of MapReduce the tuples are
sub-tasks (maps or reduces) which contain both the code and data for
the task.

The master job object divvies up the starting data and creates a pile
of map tasks, and writes all these to the tuplespace. Workers take
jobs off the tuplespace, run them, and write the results back to the
tuplespace. The master job reads these, re-partitions the intermediate
data, and creates a pile of reduce jobs. Rinse and repeat.

It's just as easy to see what's going on by looking at the code. Open
map\_reduce\_job.rb and follow along. The high-level "run" method does
just what I discussed above:

    # from map_reduce_job.rb
    def run
      map_data = Partitioner::simple_partition_data(@data, @num_map_tasks)
      map_tasks = Array.new

      (0..@num_map_tasks - 1).each do |i|
        map_tasks << WorkerTask.new(i + 1, map_data[i], @map)
      end

      map_data = run_tasks(map_tasks)

      reduce_data = @partition.call(map_data, @num_reduce_tasks)
      reduce_tasks = Array.new
  
      (0..@num_reduce_tasks - 1).each do |i|
        reduce_tasks << WorkerTask.new(i + 1, reduce_data[i], @reduce)
      end

      run_tasks(reduce_tasks)
    end

Each WorkerTask has three attributes: an ID (just an integer), the
data (any type), and the code (a lambda). These are serialized
automatically by DRb, the framework underlying Rinda.

Writing tasks to the tuplespace and getting their results is simple:

    # from map_reduce_job.rb
    def run_tasks(tasks)
      result = Array.new
  
      tasks.each do |task|
        @tuplespace.write(['task', DRb.uri, task])
      end

      tasks.each_with_index do |task, i|
        tuple = @tuplespace.take(['result', DRb.uri, task.task_id, nil])
        result[i] = tuple[3]
      end
  
      result
    end

The key operations for interacting with the tuplespace are write() and
take(). For writing, the array parameter is a tuple which contains
pretty much whatever you want. I use the string 'task' as the first
parameter and the worker (below) looks for that. The second is our
process's DRb URI, which the worker puts in the result tuple so only
our process reads it. Then I send the task as the third parameter.

Let's skip to the worker code before continuing, since it matches up
with the run_tasks() shown above:

    # from worker.rb
    loop do
      tuple = tuplespace.take(['task', nil, nil])
      task = tuple[2]
  
      result = task.run
  
      tuplespace.write(['result', tuple[1], task.task_id, result])
    end

The take() operation looks for tuples that are as specific as you
desire. In the worker, it looks for anything that's identified as
'task' with two more entries. (Nil entries are wildcards, i.e. "match
anything.") The job submitted the task as tuple[2], so the worker gets
the task in the same spot: tuple[2].

The worker runs the task, then writes a 'result' tuple. The result
tuple is more specific, though: it uses tuple[2] for the task ID and
tuple[3] for the data. This allows the job to look for the result
tuple matching a specific task ID. (See the take() line in the job.)


Partitioning
------------

Up to now I skipped over partitioning. This varies depending on the
type of job, but in the examples I've worked through so far, I've seen
the following pattern: First, for the incoming data, just split it up
semi-evenly. I use an array, e.g. for the URL counting example:

    job.data = File::readlines('logfile.txt')

Then the lines are divvied up across the map jobs. (See
simple\_partition\_data().)

Second, the intermediate data (coming from map, going to reduce) is
usually best shuffled such that all data sharing a common key goes to
the same reduce task. In the URL counting example, the return value of
map is [url, count] pairs, so the partitioner uses *hash(url) mod R*
(where R is the number of reduce tasks) to distribute pairs for
reduction.

The MapReduceJob object uses a lambda for the intermediate
partitioning so that, like map and reduce, the user can provide custom
behavior as desired. The partitioner is run in the master process, so
it doesn't need to be a lambda, but I stuck with that for consistency.
I provide a couple algorithms pre-built in the "Partitioner" module.


Performance
-----------

As stated upfront, my goal was not to create a high-performance
distributed/parallel processing system. This is *simple* MapReduce.
See the Google paper for *fast* MapReduce. I believe, however, that
one could use my simple framework with some tricks to make things
somewhat fast.

First, the map and reduce code doesn't need to be the *real* code. It
could just spawn a process using IO::popen() and run a task written in
a fast language like C or OCaml.

Second, the data sent to the map and reduce tasks doesn't need to be
the *real* data. It could just be a locator (e.g. file name, URL) that
the task uses to load the data. A simple solution could use files
located on a network share or SAN. A faster solution would have data
on the worker machines already.

Some of the simple examples presented in the Google paper will
actually run *slower* on multiple machines unless the data starts on
the workers. The problems are I/O-bound already, so adding network
overhead to shuffle the data around just makes it worse.

One could improve MapReduceJob to maximize locality of data for the
tasks. Right now any random worker will grab tasks from the
tuplespace. If you arranged for slices of data to reside on workers
ahead of time, the master job could tell workers to only take tasks
dealing with that data. For example, run_tasks() might include a
worker ID when it writes tasks:

    # master
    @tuplespace.write(['task', worker_id, DRb.uri, task])

And the worker would take tasks meant for it:

    # worker
    tuple = @tuplespace.take(['task', my_worker_id, nil, nil])
    task = tuple[3]

Of course, if the workers are going to have slices of data spread
among them, they should collect that data in a distributed fashion,
too. Sounds like a good MapReduce job, doesn't it?


Reliability
-----------

Another key component of Google's MapReduce is handling failure of
workers, a likely occurrence when you have thousands of machines
clustered together. My simple MapReduce currently does not handle
workers going away mid-job. Nor does it handle the "bad record" case
where workers repeatedly crash on a slice of input data.

Rinda supports expiration of tuples, so it would not be difficult to
augment this MapReduce to support worker failure, and I may do so in
the future.


Conclusion
----------

My initial reaction to reading Dean and Ghemawat's excellent paper
was, "I bet I could create a simple version of this in Ruby pretty
easy." And, in fact, I could. My simple Ruby version covers the high
points with surprisingly little code.

I've implemented several of the examples from the paper, one presented
here and two more in the unit tests. Performance stinks and that's
partly for reasons already discussed. The other reason is that the
examples are overly simple: e.g. with URL counting you expend almost
as much effort reading the lines into an array as you'd expend by
counting the URLs right there.

Most importantly, I wanted to build a framework where I could
experiment with writing MapReduce jobs, and I think I succeeded well.
The code to create jobs is about as simple as it could be. As my spare
time permits, I hope to write additional articles covering jobs
written in MapReduce style.

Finally, I hope this framework is a valuable learning tool for others
to experiment with, and I welcome comments and suggestions from
readers who try it out.

License
-------

Copyright 2006 Josh Carter. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following
   disclaimer in the documentation and/or other materials provided
   with the distribution.

THIS SOFTWARE IS PROVIDED BY JOSH CARTER ``AS IS'' AND ANY EXPRESS OR
IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL JOSH CARTER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation
are those of the authors and should not be interpreted as representing
official policies, either expressed or implied, of Josh Carter.
