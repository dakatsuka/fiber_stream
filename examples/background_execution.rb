# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "async"
require "fiber_stream"

jobs = [
  { id: "import", delay: 0.12 },
  { id: "validate", delay: 0.05 },
  { id: "publish", delay: 0.08 }
]

Sync do
  running =
    FiberStream::Source.each(jobs)
      .parallel_map(concurrency: 2) do |job|
        sleep job.fetch(:delay)
        "#{job.fetch(:id)} complete"
      end
      .to(FiberStream::Sink.to_a)
      .run_async

  3.times do |tick|
    sleep 0.03
    puts "foreground tick #{tick + 1}"
  end

  puts "Background result"
  running.wait.each { |line| puts "- #{line}" }
end
