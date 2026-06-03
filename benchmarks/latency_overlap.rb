# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "async"
require "fiber_stream"
require "optparse"

options = {
  items: 12,
  delay: 0.03,
  concurrency: 4,
  buffer: 4
}

OptionParser.new do |parser|
  parser.banner = "Usage: bundle exec ruby benchmarks/latency_overlap.rb [options]"
  parser.on("--items COUNT", Integer, "Input item count") { |value| options[:items] = value }
  parser.on("--delay SECONDS", Float, "Per-step delay") { |value| options[:delay] = value }
  parser.on("--concurrency COUNT", Integer, "Concurrent delayed jobs") { |value| options[:concurrency] = value }
  parser.on("--buffer COUNT", Integer, "FiberStream buffer size") { |value| options[:buffer] = value }
end.parse!

def run_case(label)
  started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  result = yield
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
  [label, elapsed, result]
end

def print_results(title, results)
  fastest = results.map { |(_, elapsed, _)| elapsed }.min

  puts title
  puts format("%-34s %10s %10s", "case", "seconds", "slower")
  puts "-" * 58
  results.each do |label, elapsed, _|
    puts format("%-34s %10.4f %9.2fx", label, elapsed, elapsed / fastest)
  end
  puts
end

def delayed_job(value, delay)
  sleep delay
  value * 2
end

def produce(value, delay)
  sleep delay
  value
end

def consume(total, value, delay)
  sleep delay
  total + value
end

values = (1..options.fetch(:items)).to_a
delay = options.fetch(:delay)

job_results = []

job_results << run_case("Enumerable serial map") do
  values.map { |value| delayed_job(value, delay) }
end

job_results << run_case("Direct Async tasks") do
  Sync do
    values
      .each_slice(options.fetch(:concurrency))
      .flat_map do |slice|
        slice.map { |value| Async { delayed_job(value, delay) } }.map(&:wait)
      end
  end
end

job_results << run_case("FiberStream parallel_map") do
  Sync do
    FiberStream::Source.each(values)
      .parallel_map(concurrency: options.fetch(:concurrency)) { |value| delayed_job(value, delay) }
      .run_with(FiberStream::Sink.to_a)
  end
end

expected_jobs = job_results.fetch(0).fetch(2)
job_results.each do |label, _, result|
  raise "#{label} produced a different result" unless result == expected_jobs
end

pipeline_results = []

pipeline_results << run_case("Enumerable serial producer/consumer") do
  values
    .map { |value| produce(value, delay) }
    .inject(0) { |total, value| consume(total, value, delay) }
end

pipeline_results << run_case("FiberStream no buffer") do
  FiberStream::Source.each(values)
    .map { |value| produce(value, delay) }
    .run_with(FiberStream::Sink.fold(0) { |total, value| consume(total, value, delay) })
end

pipeline_results << run_case("FiberStream buffer") do
  Sync do
    FiberStream::Source.each(values)
      .map { |value| produce(value, delay) }
      .buffer(options.fetch(:buffer))
      .run_with(FiberStream::Sink.fold(0) { |total, value| consume(total, value, delay) })
  end
end

expected_pipeline = pipeline_results.fetch(0).fetch(2)
pipeline_results.each do |label, _, result|
  raise "#{label} produced a different result" unless result == expected_pipeline
end

puts "Ruby #{RUBY_VERSION}"
puts "items=#{options.fetch(:items)} delay=#{delay}s " \
     "concurrency=#{options.fetch(:concurrency)} buffer=#{options.fetch(:buffer)}"
puts
print_results("Independent delayed jobs", job_results)
print_results("Producer/consumer overlap", pipeline_results)
