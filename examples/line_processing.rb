# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "fiber_stream"

log_chunks = [
  "INFO boot\nWARN slow query",
  "\nERROR failed job\n",
  "INFO recovered"
]

warnings_and_errors =
  FiberStream::Source.each(log_chunks)
    .lines
    .select { |line| line.start_with?("WARN", "ERROR") }
    .run_with(FiberStream::Sink.to_a)

puts "Warnings and errors"
warnings_and_errors.each { |line| puts "- #{line}" }
