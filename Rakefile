require 'rake/testtask'

Rake::TestTask.new do |t|
  t.libs << "test"
  t.test_files = FileList['test/test*.rb']
  t.verbose = true
end

task :rinda do
  system "ruby ringserver.rb &"
  system "ruby tuplespace.rb &"
end

task :worker do
  exec "ruby worker.rb"
end