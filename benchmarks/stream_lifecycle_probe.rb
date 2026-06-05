# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "async"
require "fiber_stream"
require "objspace"
require "optparse"

options = {
  iterations: 200,
  warmup: 20,
  items: 64,
  sample_every: 25,
  parallel_concurrency: 2,
  buffer_size: 4,
  ractor_workers: 2,
  cases: %w[
    parallel_map
    buffer
    async
    ractor_port
    run_async
    ractor_map
  ],
  max_rss_growth_kb: nil,
  max_live_slot_growth: nil,
  max_thread_growth: nil,
  max_fiber_growth: nil,
  max_boundary_growth: nil
}

OptionParser.new do |parser|
  parser.banner = "Usage: bundle exec ruby benchmarks/stream_lifecycle_probe.rb [options]"
  parser.on("--iterations COUNT", Integer, "Measured iterations") { |value| options[:iterations] = value }
  parser.on("--warmup COUNT", Integer, "Warmup iterations before baseline") { |value| options[:warmup] = value }
  parser.on("--items COUNT", Integer, "Input items per iteration") { |value| options[:items] = value }
  parser.on("--sample-every COUNT", Integer, "Print a sample every COUNT iterations") do |value|
    options[:sample_every] = value
  end
  parser.on("--parallel-concurrency COUNT", Integer, "Flow.parallel_map concurrency") do |value|
    options[:parallel_concurrency] = value
  end
  parser.on("--buffer-size COUNT", Integer, "Flow.buffer size") { |value| options[:buffer_size] = value }
  parser.on("--ractor-workers COUNT", Integer, "Flow.ractor_map workers") { |value| options[:ractor_workers] = value }
  parser.on("--cases LIST", String, "Comma-separated case groups to run") do |value|
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
  parser.on(
    "--max-thread-growth COUNT",
    Integer,
    "Exit non-zero if final alive thread growth is above COUNT"
  ) do |value|
    options[:max_thread_growth] = value
  end
  parser.on("--max-fiber-growth COUNT", Integer, "Exit non-zero if final alive fiber growth is above COUNT") do |value|
    options[:max_fiber_growth] = value
  end
  parser.on("--max-boundary-growth COUNT", Integer, "Exit non-zero if final boundary growth is above COUNT") do |value|
    options[:max_boundary_growth] = value
  end
end.parse!

module LifecycleProbe
  module_function

  BOUNDARY_CLASSES = [
    FiberStream.const_get(:Pull).__send__(:const_get, :ParallelMapBoundary),
    FiberStream.const_get(:Pull).__send__(:const_get, :BufferBoundary),
    FiberStream.const_get(:Pull).__send__(:const_get, :AsyncBoundary),
    FiberStream.const_get(:Pull).__send__(:const_get, :RactorPortSource),
    FiberStream.const_get(:Pull).__send__(:const_get, :RactorMapBoundary)
  ].freeze

  RACTOR_IDENTITY = Ractor.shareable_proc { |value| value }
  RACTOR_DOUBLE = Ractor.shareable_proc { |value| value * 2 }
  RACTOR_FAILING =
    Ractor.shareable_proc do |value|
      raise "probe ractor failure" if value == 3

      value
    end
  VALID_CASES = %w[parallel_map buffer async ractor_port run_async ractor_map].freeze

  def validate_positive!(options, key)
    return if options.fetch(key).positive?

    raise OptionParser::InvalidArgument, "#{key} must be positive"
  end

  def validate_non_negative!(options, key)
    value = options.fetch(key)
    return if value.nil? || value >= 0

    raise OptionParser::InvalidArgument, "#{key} must be non-negative"
  end

  def validate_options!(options)
    %i[
      iterations
      warmup
      items
      sample_every
      parallel_concurrency
      buffer_size
      ractor_workers
    ].each { |key| validate_positive!(options, key) }

    %i[
      max_rss_growth_kb
      max_live_slot_growth
      max_thread_growth
      max_fiber_growth
      max_boundary_growth
    ].each { |key| validate_non_negative!(options, key) }

    unknown_cases = options.fetch(:cases) - VALID_CASES
    raise OptionParser::InvalidArgument, "unknown cases: #{unknown_cases.join(',')}" unless unknown_cases.empty?
    raise OptionParser::InvalidArgument, "cases must not be empty" if options.fetch(:cases).empty?

    return unless options.fetch(:max_rss_growth_kb)
    return if File.file?("/proc/self/status")

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

  def count_alive_fibers
    ObjectSpace.each_object(Fiber).count do |fiber|
      fiber.alive?
    rescue FiberError
      false
    end
  end

  def count_boundaries
    BOUNDARY_CLASSES.sum { |klass| count_objects(klass) }
  end

  def snapshot
    gc = GC.stat
    {
      rss_kb: rss_kb,
      heap_live_slots: gc.fetch(:heap_live_slots),
      old_objects: gc.fetch(:old_objects),
      total_allocated_objects: gc.fetch(:total_allocated_objects),
      total_freed_objects: gc.fetch(:total_freed_objects),
      boundaries: count_boundaries,
      queues: count_objects(Thread::Queue),
      threads: Thread.list.count(&:alive?),
      fibers: count_alive_fibers,
      ractors: count_objects(Ractor),
      running_pipelines: count_objects(FiberStream::RunningPipeline)
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
      "%10s %10s %10s %10s %10s %10s %8s %8s %8s %8s %8s",
      label,
      format_delta(current, baseline, :rss_kb),
      format_delta(current, baseline, :heap_live_slots),
      format_delta(current, baseline, :old_objects),
      format_delta(current, baseline, :total_allocated_objects),
      format_delta(current, baseline, :total_freed_objects),
      current.fetch(:boundaries),
      current.fetch(:queues),
      current.fetch(:threads),
      current.fetch(:fibers),
      current.fetch(:running_pipelines)
    )
  end

  def input_values(count)
    1.upto(count).to_a
  end

  def failing_values(count)
    input_values([count, 3].max)
  end

  def run_parallel_map_group(options)
    values = input_values(options.fetch(:items))
    failing = failing_values(options.fetch(:items))
    concurrency = options.fetch(:parallel_concurrency)

    Sync do
      FiberStream::Source.each(values)
        .parallel_map(concurrency: concurrency) { |value| value * 2 }
        .run_with(FiberStream::Sink.fold(0) { |sum, value| sum + value })

      FiberStream::Source.each(values)
        .parallel_map(concurrency: concurrency) { |value| value }
        .run_with(FiberStream::Sink.first)

      begin
        FiberStream::Source.each(failing)
          .parallel_map(concurrency: concurrency) do |value|
            raise "probe parallel_map failure" if value == 3

            value
          end
          .run_with(FiberStream::Sink.to_a)
      rescue RuntimeError
        nil
      end
    end
  end

  def run_buffer_group(options)
    values = input_values(options.fetch(:items))
    failing = failing_values(options.fetch(:items))
    size = options.fetch(:buffer_size)

    Sync do
      FiberStream::Source.each(values)
        .buffer(size)
        .run_with(FiberStream::Sink.to_a)

      FiberStream::Source.each(values)
        .buffer(size)
        .run_with(FiberStream::Sink.first)

      begin
        FiberStream::Source.each(failing)
          .map do |value|
            raise "probe buffer upstream failure" if value == 3

            value
          end
          .buffer(size)
          .run_with(FiberStream::Sink.to_a)
      rescue RuntimeError
        nil
      end
    end
  end

  def run_async_group(options)
    values = input_values(options.fetch(:items))
    failing = failing_values(options.fetch(:items))

    Sync do
      FiberStream::Source.each(values)
        .async
        .run_with(FiberStream::Sink.to_a)

      FiberStream::Source.each(values)
        .async
        .run_with(FiberStream::Sink.first)

      begin
        FiberStream::Source.each(failing)
          .map do |value|
            raise "probe async upstream failure" if value == 3

            value
          end
          .async
          .run_with(FiberStream::Sink.to_a)
      rescue RuntimeError
        nil
      end
    end
  end

  def spawn_ractor_port_producer(values, mode)
    data_port = Ractor::Port.new
    setup_port = Ractor::Port.new
    producer =
      Ractor.new(data_port, setup_port, values, mode) do |outbox, setup, items, producer_mode|
        inbox = Ractor::Port.new
        setup.send(inbox)
        sent = 0

        loop do
          control = inbox.receive

          case control
          in FiberStream::RactorPort::Ack
            if producer_mode == :failure
              outbox.send(FiberStream::RactorPort::Failure.new("RuntimeError", "probe producer failure"))
              break [:failure, sent]
            elsif sent < items.length
              outbox.send(FiberStream::RactorPort::Element.new(items.fetch(sent)))
              sent += 1
            else
              outbox.send(FiberStream::RactorPort::Complete.new)
              break [:completed, sent]
            end
          in FiberStream::RactorPort::Cancel[reason]
            break [:cancelled, sent, reason]
          else
            break [:invalid_control, control.class.name]
          end
        end
      end

    ack_port = setup_port.receive
    [data_port, ack_port, producer]
  end

  def run_ractor_port_group(options)
    values = input_values(options.fetch(:items))

    data_port, ack_port, producer = spawn_ractor_port_producer(values, :normal)
    FiberStream::Source.ractor_port(data_port, ack_port: ack_port)
      .run_with(FiberStream::Sink.to_a)
    producer.value

    data_port, ack_port, producer = spawn_ractor_port_producer(values, :normal)
    FiberStream::Source.ractor_port(data_port, ack_port: ack_port)
      .run_with(FiberStream::Sink.first)
    producer.value

    data_port, ack_port, producer = spawn_ractor_port_producer(values, :failure)
    begin
      FiberStream::Source.ractor_port(data_port, ack_port: ack_port)
        .run_with(FiberStream::Sink.to_a)
    rescue FiberStream::RactorPortSourceError
      nil
    end
    producer.value
  end

  def run_run_async_group(options)
    values = input_values(options.fetch(:items))
    slow_values = input_values([options.fetch(:items), 64].max)
    failing = failing_values(options.fetch(:items))

    Sync do
      FiberStream::Source.each(values)
        .to(FiberStream::Sink.to_a)
        .run_async
        .wait

      begin
        FiberStream::Source.each(failing)
          .map do |value|
            raise "probe run_async failure" if value == 3

            value
          end
          .to(FiberStream::Sink.to_a)
          .run_async
          .wait
      rescue RuntimeError
        nil
      end

      running =
        FiberStream::Source.each(slow_values)
          .map do |value|
            sleep 0.001
            value
          end
          .to(FiberStream::Sink.to_a)
          .run_async

      sleep 0.002
      running.cancel
      begin
        running.wait
      rescue FiberStream::PipelineCancelledError
        nil
      end
    end
  end

  def run_ractor_map_group(options)
    values = input_values(options.fetch(:items))
    failing = failing_values(options.fetch(:items))
    workers = options.fetch(:ractor_workers)

    FiberStream::Source.each(values)
      .ractor_map(workers: workers, &RACTOR_DOUBLE)
      .run_with(FiberStream::Sink.fold(0) { |sum, value| sum + value })

    FiberStream::Source.each(values)
      .ractor_map(workers: workers, &RACTOR_IDENTITY)
      .run_with(FiberStream::Sink.first)

    begin
      FiberStream::Source.each(failing)
        .ractor_map(workers: workers, &RACTOR_FAILING)
        .run_with(FiberStream::Sink.to_a)
    rescue FiberStream::RactorMapError
      nil
    end
  end

  def run_case_group(name, options)
    case name
    when "parallel_map"
      run_parallel_map_group(options)
    when "buffer"
      run_buffer_group(options)
    when "async"
      run_async_group(options)
    when "ractor_port"
      run_ractor_port_group(options)
    when "run_async"
      run_run_async_group(options)
    when "ractor_map"
      run_ractor_map_group(options)
    else
      raise ArgumentError, "unknown case group: #{name}"
    end
  end

  def run_all_case_groups(options)
    options.fetch(:cases).each { |name| run_case_group(name, options) }
  end

  def check_thresholds!(options, final, baseline)
    failures = []

    check_threshold(failures, "RSS growth", delta(final, baseline, :rss_kb), options.fetch(:max_rss_growth_kb), "KB")
    check_threshold(
      failures,
      "heap_live_slots growth",
      delta(final, baseline, :heap_live_slots),
      options.fetch(:max_live_slot_growth),
      "slots"
    )
    check_threshold(failures, "thread growth", delta(final, baseline, :threads), options.fetch(:max_thread_growth), "")
    check_threshold(failures, "fiber growth", delta(final, baseline, :fibers), options.fetch(:max_fiber_growth), "")
    check_threshold(
      failures,
      "boundary growth",
      delta(final, baseline, :boundaries),
      options.fetch(:max_boundary_growth),
      ""
    )

    abort failures.join("\n") unless failures.empty?
  end

  def check_threshold(failures, label, value, limit, unit)
    return unless limit && value && value > limit

    suffix = unit.empty? ? "" : " #{unit}"
    failures << "#{label} #{value}#{suffix} exceeded #{limit}#{suffix}"
  end

  def run(options)
    validate_options!(options)
    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    puts "Ruby #{RUBY_VERSION}"
    puts "iterations=#{options.fetch(:iterations)} warmup=#{options.fetch(:warmup)} " \
         "items=#{options.fetch(:items)} cases=#{options.fetch(:cases).join(',')}"
    puts "parallel_concurrency=#{options.fetch(:parallel_concurrency)} " \
         "buffer_size=#{options.fetch(:buffer_size)} ractor_workers=#{options.fetch(:ractor_workers)}"
    puts

    options.fetch(:warmup).times { run_all_case_groups(options) }
    force_gc
    baseline = snapshot

    puts format(
      "%10s %10s %10s %10s %10s %10s %8s %8s %8s %8s %8s",
      "sample",
      "rss_kb",
      "live",
      "old",
      "alloc",
      "freed",
      "bounds",
      "queues",
      "threads",
      "fibers",
      "running"
    )
    puts "-" * 120
    print_sample("baseline", baseline, baseline)

    1.upto(options.fetch(:iterations)) do |iteration|
      run_all_case_groups(options)
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
         "queues=#{final.fetch(:queues)} threads=#{final.fetch(:threads)} " \
         "fibers=#{final.fetch(:fibers)} running_pipelines=#{final.fetch(:running_pipelines)}"
    puts format("Elapsed: %.3fs", elapsed)

    check_thresholds!(options, final, baseline)
  end
end

LifecycleProbe.run(options)
