# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "async"
require "async/http/internet/instance"
require "fiber_stream"

DEFAULT_URL =
  "https://raw.githubusercontent.com/elastic/examples/master/" \
  "Common%20Data%20Formats/nginx_logs/nginx_logs"

URL = ENV.fetch("FIBER_STREAM_HTTP_LOG_URL", DEFAULT_URL)
PROGRESS_EVERY = Integer(ENV.fetch("FIBER_STREAM_HTTP_PROGRESS_EVERY", "10_000"))

LOG_LINE =
  /
    \A
    (?<remote_addr>\S+)\s+\S+\s+\S+\s+
    \[[^\]]+\]\s+
    "[^"]+"\s+
    (?<status>\d{3})\s+
    (?<bytes>\d+|-)\s
  /x

def monotonic_time
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def parse_access_log(line)
  match = LOG_LINE.match(line)
  return nil unless match

  {
    remote_addr: match[:remote_addr],
    status: match[:status],
    bytes: match[:bytes] == "-" ? 0 : match[:bytes].to_i
  }
end

def empty_stats
  {
    lines: 0,
    parsed: 0,
    payload_bytes: 0,
    statuses: Hash.new(0),
    remote_addrs: Hash.new(0),
    started_at: monotonic_time
  }
end

def record_entry(stats, entry)
  stats[:lines] += 1

  if entry
    stats[:parsed] += 1
    stats[:payload_bytes] += entry.fetch(:bytes)
    stats[:statuses][entry.fetch(:status)] += 1
    stats[:remote_addrs][entry.fetch(:remote_addr)] += 1
  end

  if (stats[:lines] % PROGRESS_EVERY).zero?
    elapsed = monotonic_time - stats.fetch(:started_at)
    puts format(
      "processed %<lines>d lines in %<elapsed>.2fs",
      lines: stats.fetch(:lines),
      elapsed: elapsed
    )
  end

  stats
end

def print_summary(stats)
  elapsed = monotonic_time - stats.fetch(:started_at)
  mib = stats.fetch(:payload_bytes).fdiv(1024 * 1024)

  puts
  puts "Streaming HTTP body summary"
  puts "URL: #{URL}"
  puts format("lines parsed: %<parsed>d/%<lines>d", stats)
  puts format("logged payload bytes: %<mib>.2f MiB", mib: mib)
  puts format("unique remote addresses: %<count>d", count: stats.fetch(:remote_addrs).length)
  puts format("elapsed: %<elapsed>.2fs", elapsed: elapsed)

  puts
  puts "HTTP status counts"
  stats.fetch(:statuses).sort.each do |status, count|
    puts format("- %<status>s: %<count>d", status: status, count: count)
  end
end

stats = empty_stats

processed =
  Sync do
    Async::HTTP::Internet.get(URL) do |response|
      unless response.status == 200
        raise "unexpected HTTP status #{response.status} for #{URL}"
      end

      FiberStream::Source.each(response.body)
        .lines(max_length: 16 * 1024)
        .map { |line| parse_access_log(line) }
        .run_with(
          FiberStream::Sink.foreach do |entry|
            record_entry(stats, entry)
          end
        )
    end
  end

raise "processed count mismatch" unless processed == stats.fetch(:lines)

print_summary(stats)
