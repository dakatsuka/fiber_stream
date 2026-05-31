# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "async"
require "fiber_stream"

ITEM_COUNT = 8
BUFFER_SIZE = 3
PRODUCER_DELAY = 0.05
CONSUMER_DELAY = 0.20

def run_pipeline(label, buffer_size: nil)
  started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  produced = 0
  consumed = 0
  max_produced_ahead = 0

  log = lambda do |event, item|
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
    produced_ahead = produced - consumed
    max_produced_ahead = [max_produced_ahead, produced_ahead].max

    puts format(
      "%<elapsed>6.2fs  %-10<event>s item=%<item>2d produced_ahead=%<ahead>d",
      elapsed: elapsed,
      event: event,
      item: item,
      ahead: produced_ahead
    )
  end

  source =
    FiberStream::Source.each(1..ITEM_COUNT)
      .map do |item|
        produced += 1
        log.call("produce", item)
        sleep PRODUCER_DELAY
        item
      end

  source = source.buffer(buffer_size) if buffer_size

  result =
    source.run_with(
      FiberStream::Sink.fold([]) do |items, item|
        log.call("consume", item)
        sleep CONSUMER_DELAY
        consumed += 1
        items << item
      end
    )

  puts "#{label}: result=#{result.inspect}"
  puts "#{label}: max produced ahead=#{max_produced_ahead}"
  puts
end

Sync do
  puts "Unbuffered: downstream demand gates upstream one item at a time."
  run_pipeline("unbuffered")

  puts "buffer(#{BUFFER_SIZE}): upstream can prefetch, but backpressure keeps it bounded."
  puts "produced_ahead includes queued values plus producer/consumer in-flight work."
  run_pipeline("buffered", buffer_size: BUFFER_SIZE)
end
