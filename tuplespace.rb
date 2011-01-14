#!/usr/bin/env ruby -w

# Registering a TupleSpace with Rinda::Ring

require 'rinda/ring'
require 'rinda/tuplespace'

DRb.start_service

provider = Rinda::RingProvider.new(:TupleSpace, Rinda::TupleSpace.new, 'Tuple Space')
provider.provide

DRb.thread.join