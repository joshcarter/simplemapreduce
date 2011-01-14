#
# Copyright (c) 2006 Josh Carter <josh@multipart-mixed.com>
#
# All rights reserved.  You can redistribute and/or modify this under
# the same terms as Ruby.
#

# Simple class used for map and reduce tasks. MapReducetask creates
# these and submits them to workers.
#
class WorkerTask
  attr_reader :task_id, :data, :process
  
  def initialize(task_id, data, process)
    @task_id = task_id
    @data    = data
    @process = process
  end
  
  def run
    @process.call @data
  end
end
