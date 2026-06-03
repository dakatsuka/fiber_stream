# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "async"
require "digest"
require "fiber_stream"
require "optparse"

options = {
  items: 1_000,
  take: 120,
  work: 25,
  concurrency: 4,
  workers: 2
}

OptionParser.new do |parser|
  parser.banner = "Usage: bundle exec ruby benchmarks/stream_transform.rb [options]"
  parser.on("--items COUNT", Integer, "Input item count") { |value| options[:items] = value }
  parser.on("--take COUNT", Integer, "Number of output values to keep") { |value| options[:take] = value }
  parser.on("--work COUNT", Integer, "SHA-256 rounds per item") { |value| options[:work] = value }
  parser.on("--concurrency COUNT", Integer, "FiberStream parallel_map concurrency") do |value|
    options[:concurrency] = value
  end
  parser.on("--workers COUNT", Integer, "FiberStream ractor_map workers") { |value| options[:workers] = value }
end.parse!

TRANSFORM =
  Ractor.shareable_proc do |input|
    value, work = input
    text = "#{value}:#{value * 31}:#{value * 131}"
    work.times.reduce(text) { |digest, _| Digest::SHA256.hexdigest(digest) }
  end

def keep_digest?(digest)
  digest.getbyte(0).even?
end

def run_case(label)
  started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  result, transformed = yield
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
  [label, elapsed, result, transformed]
end

def print_results(results)
  fastest = results.map { |(_, elapsed, _, _)| elapsed }.min

  puts format("%-30s %10s %10s %8s %11s", "case", "seconds", "slower", "items", "transforms")
  puts "-" * 75
  results.each do |label, elapsed, result, transformed|
    puts format(
      "%-30s %10.4f %9.2fx %8d %11d",
      label,
      elapsed,
      elapsed / fastest,
      result.length,
      transformed
    )
  end
end

values = (1..options.fetch(:items)).map { |value| [value, options.fetch(:work)] }

results = []

results << run_case("Enumerable eager") do
  transformed = 0
  values
    .map { |value| TRANSFORM.call(value) }
    .tap { transformed = values.length }
    .select { |digest| keep_digest?(digest) }
    .take(options.fetch(:take))
    .then { |result| [result, transformed] }
end

results << run_case("Enumerable::Lazy") do
  transformed = 0
  values
    .lazy
    .map do |value|
      transformed += 1
      TRANSFORM.call(value)
    end
    .select { |digest| keep_digest?(digest) }
    .take(options.fetch(:take))
    .force
    .then { |result| [result, transformed] }
end

results << run_case("FiberStream linear") do
  transformed = 0
  FiberStream::Source.each(values)
    .map do |value|
      transformed += 1
      TRANSFORM.call(value)
    end
    .select { |digest| keep_digest?(digest) }
    .take(options.fetch(:take))
    .run_with(FiberStream::Sink.to_a)
    .then { |result| [result, transformed] }
end

results << run_case("FiberStream parallel_map") do
  transformed = 0
  Sync do
    FiberStream::Source.each(values)
      .parallel_map(concurrency: options.fetch(:concurrency)) do |value|
        transformed += 1
        TRANSFORM.call(value)
      end
      .select { |digest| keep_digest?(digest) }
      .take(options.fetch(:take))
      .run_with(FiberStream::Sink.to_a)
  end
    .then { |result| [result, transformed] }
end

results << run_case("FiberStream ractor_map") do
  transformed = 0
  FiberStream::Source.each(values)
    .map do |value|
      transformed += 1
      value
    end
    .ractor_map(workers: options.fetch(:workers), &TRANSFORM)
    .select { |digest| keep_digest?(digest) }
    .take(options.fetch(:take))
    .run_with(FiberStream::Sink.to_a)
    .then { |result| [result, transformed] }
end

expected = results.fetch(0).fetch(2)
results.each do |label, _, result, _|
  raise "#{label} produced a different result" unless result == expected
end

puts "Ruby #{RUBY_VERSION}"
puts "items=#{options.fetch(:items)} take=#{options.fetch(:take)} work=#{options.fetch(:work)} " \
     "parallel_map=#{options.fetch(:concurrency)} ractor_map=#{options.fetch(:workers)}"
puts
print_results(results)
