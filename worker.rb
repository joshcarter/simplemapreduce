#!/usr/bin/env ruby -w
#
# Copyright (c) 2006 Josh Carter <josh@multipart-mixed.com>
#
# All rights reserved.  You can redistribute and/or modify this under
# the same terms as Ruby.
#

require 'rinda/ring'
require 'worker_task'

# Start Rinda and find TupleSpace
#
DRb.start_service
ring_server = Rinda::RingFinger.primary

ts = ring_server.read([:name, :TupleSpace, nil, nil])[2]
ts = Rinda::TupleSpaceProxy.new ts

# Wait for tasks, pull them off and run them
#
loop do
  tuple = ts.take(['task', nil, nil])
  task = tuple[2]
  print "taking task #{task.task_id}... "
  
  result = task.run
  puts "done"
  
  ts.write(['result', tuple[1], task.task_id, result])
end