# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "fiber_stream"
require "objspace"
require "optparse"

options = {
  iterations: 200,
  warmup: 20,
  items: 64,
  workers: 2,
  sample_every: 25,
  cases: %w[normal early_close mapper_failure input_failure output_failure move],
  max_rss_growth_kb: nil,
  max_live_slot_growth: nil
}

VALID_CASES = %w[normal early_close mapper_failure input_failure output_failure move].freeze

OptionParser.new do |parser|
  parser.banner = "Usage: bundle exec ruby benchmarks/ractor_map_leak_probe.rb [options]"
  parser.on("--iterations COUNT", Integer, "Measured iterations") { |value| options[:iterations] = value }
  parser.on("--warmup COUNT", Integer, "Warmup iterations before baseline") { |value| options[:warmup] = value }
  parser.on("--items COUNT", Integer, "Input items per iteration") { |value| options[:items] = value }
  parser.on("--workers COUNT", Integer, "ractor_map worker count") { |value| options[:workers] = value }
  parser.on("--sample-every COUNT", Integer, "Print a sample every COUNT iterations") do |value|
    options[:sample_every] = value
  end
  parser.on("--cases LIST", String, "Comma-separated cases to run") do |value|
    options[:cases] = value.split(",").map(&:strip).reject(&:empty?)
  end
  parser.on("--max-rss-growth-kb COUNT", Integer, "Exit non-zero if final RSS growth is above COUNT KB") do |value|
    options[:max_rss_growth_kb] = value
  end
  parser.on(
    "--max-live-slot-growth COUNT",
    Integer,
    "Exit non-zero if final heap_live_slots growth is above COUNT"
  ) do |value|
    options[:max_live_slot_growth] = value
  end
end.parse!

BOUNDARY_CLASS = FiberStream.const_get(:Pull).__send__(:const_get, :RactorMapBoundary)

IDENTITY_MAPPER = Ractor.shareable_proc { |value| value }
DOUBLE_MAPPER = Ractor.shareable_proc { |value| value * 2 }
FAILING_MAPPER =
  Ractor.shareable_proc do |value|
    raise "probe mapper failure" if value == 3

    value
  end
OUTPUT_FAILURE_MAPPER = Ractor.shareable_proc { Thread.current }
MUTATING_MAPPER =
  Ractor.shareable_proc do |value|
    value << "!"
    value
  end

def validate_positive!(options, key)
  return if options.fetch(key).positive?

  raise OptionParser::InvalidArgument, "#{key} must be positive"
end

def validate_non_negative!(options, key)
  value = options.fetch(key)
  return if value.nil? || value >= 0

  raise OptionParser::InvalidArgument, "#{key} must be non-negative"
end

%i[iterations warmup items workers sample_every].each do |key|
  validate_positive!(options, key)
end

%i[max_rss_growth_kb max_live_slot_growth].each do |key|
  validate_non_negative!(options, key)
end

unknown_cases = options.fetch(:cases) - VALID_CASES
raise OptionParser::InvalidArgument, "unknown cases: #{unknown_cases.join(',')}" unless unknown_cases.empty?
raise OptionParser::InvalidArgument, "cases must not be empty" if options.fetch(:cases).empty?

if options.fetch(:max_rss_growth_kb) && !File.file?("/proc/self/status")
  raise OptionParser::InvalidOption, "--max-rss-growth-kb requires Linux /proc/self/status"
end

def force_gc
  2.times do
    GC.start(full_mark: true, immediate_sweep: true)
    GC.compact if GC.respond_to?(:compact)
  end
end

def rss_kb
  return unless File.file?("/proc/self/status")

  File.read("/proc/self/status")[/^VmRSS:\s+(\d+)\s+kB$/, 1]&.to_i
end

def count_objects(klass)
  ObjectSpace.each_object(klass).count
end

def snapshot
  gc = GC.stat
  {
    rss_kb: rss_kb,
    heap_live_slots: gc.fetch(:heap_live_slots),
    old_objects: gc.fetch(:old_objects),
    total_allocated_objects: gc.fetch(:total_allocated_objects),
    total_freed_objects: gc.fetch(:total_freed_objects),
    boundaries: count_objects(BOUNDARY_CLASS),
    sized_queues: count_objects(Thread::SizedQueue),
    threads: Thread.list.count(&:alive?),
    ractors: count_objects(Ractor)
  }
end

def delta(snapshot, baseline, key)
  return nil if snapshot[key].nil? || baseline[key].nil?

  snapshot.fetch(key) - baseline.fetch(key)
end

def format_delta(snapshot, baseline, key)
  value = delta(snapshot, baseline, key)
  value.nil? ? "n/a" : value.to_s
end

def print_sample(label, current, baseline)
  puts format(
    "%10s %10s %10s %10s %10s %10s %8s %8s %8s",
    label,
    format_delta(current, baseline, :rss_kb),
    format_delta(current, baseline, :heap_live_slots),
    format_delta(current, baseline, :old_objects),
    format_delta(current, baseline, :total_allocated_objects),
    format_delta(current, baseline, :total_freed_objects),
    current.fetch(:boundaries),
    current.fetch(:sized_queues),
    current.fetch(:threads)
  )
end

def input_values(count)
  1.upto(count).to_a
end

def run_normal(items, workers)
  FiberStream::Source.each(input_values(items))
    .ractor_map(workers: workers, &DOUBLE_MAPPER)
    .run_with(FiberStream::Sink.fold(0) { |sum, value| sum + value })
end

def run_early_close(items, workers)
  FiberStream::Source.each(input_values(items))
    .ractor_map(workers: workers, &IDENTITY_MAPPER)
    .run_with(FiberStream::Sink.first)
end

def run_mapper_failure(items, workers)
  values = input_values([items, 3].max)

  FiberStream::Source.each(values)
    .ractor_map(workers: workers, &FAILING_MAPPER)
    .run_with(FiberStream::Sink.to_a)
rescue FiberStream::RactorMapError
  nil
end

def run_input_failure(workers)
  FiberStream::Source.each([Thread.current])
    .ractor_map(workers: workers, &IDENTITY_MAPPER)
    .run_with(FiberStream::Sink.to_a)
rescue FiberStream::RactorMapError
  nil
end

def run_output_failure(workers)
  FiberStream::Source.each([1])
    .ractor_map(workers: workers, &OUTPUT_FAILURE_MAPPER)
    .run_with(FiberStream::Sink.to_a)
rescue FiberStream::RactorMapError
  nil
end

def run_move(items, workers)
  values = Array.new(items) { +"payload" }
  FiberStream::Source.each(values)
    .ractor_map(workers: workers, input_transfer: :move, &MUTATING_MAPPER)
    .run_with(FiberStream::Sink.fold(0) { |sum, value| sum + value.bytesize })
end

def run_case(name, items, workers)
  case name
  when "normal"
    run_normal(items, workers)
  when "early_close"
    run_early_close(items, workers)
  when "mapper_failure"
    run_mapper_failure(items, workers)
  when "input_failure"
    run_input_failure(workers)
  when "output_failure"
    run_output_failure(workers)
  when "move"
    run_move(items, workers)
  else
    raise ArgumentError, "unknown case: #{name}"
  end
end

def run_all_cases(cases, items, workers)
  cases.each { |name| run_case(name, items, workers) }
end

started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

puts "Ruby #{RUBY_VERSION}"
puts "iterations=#{options.fetch(:iterations)} warmup=#{options.fetch(:warmup)} " \
     "items=#{options.fetch(:items)} workers=#{options.fetch(:workers)} cases=#{options.fetch(:cases).join(',')}"
puts

options.fetch(:warmup).times do
  run_all_cases(options.fetch(:cases), options.fetch(:items), options.fetch(:workers))
end
force_gc
baseline = snapshot

puts format(
  "%10s %10s %10s %10s %10s %10s %8s %8s %8s",
  "sample",
  "rss_kb",
  "live",
  "old",
  "alloc",
  "freed",
  "bounds",
  "queues",
  "threads"
)
puts "-" * 100
print_sample("baseline", baseline, baseline)

1.upto(options.fetch(:iterations)) do |iteration|
  run_all_cases(options.fetch(:cases), options.fetch(:items), options.fetch(:workers))
  next unless (iteration % options.fetch(:sample_every)).zero? || iteration == options.fetch(:iterations)

  force_gc
  print_sample(iteration, snapshot, baseline)
end

force_gc
final = snapshot
elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at

puts
puts "Final object counts:"
puts "  ractors=#{final.fetch(:ractors)} boundaries=#{final.fetch(:boundaries)} " \
     "queues=#{final.fetch(:sized_queues)} threads=#{final.fetch(:threads)}"
puts format("Elapsed: %.3fs", elapsed)

failures = []
rss_growth = delta(final, baseline, :rss_kb)
live_slot_growth = delta(final, baseline, :heap_live_slots)

if options.fetch(:max_rss_growth_kb) && rss_growth && rss_growth > options.fetch(:max_rss_growth_kb)
  failures << "RSS growth #{rss_growth} KB exceeded #{options.fetch(:max_rss_growth_kb)} KB"
end

if options.fetch(:max_live_slot_growth) && live_slot_growth > options.fetch(:max_live_slot_growth)
  failures << "heap_live_slots growth #{live_slot_growth} exceeded #{options.fetch(:max_live_slot_growth)}"
end

abort failures.join("\n") unless failures.empty?
