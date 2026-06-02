# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "async"
require "fiber_stream"
require "tmpdir"

source_text = <<~TEXT
  FiberStream can read from Ruby core IO objects.
  Source.io emits String chunks, and Sink.io writes them to another IO.
  This keeps stream processing explicit while preserving pull-based demand.
TEXT

Dir.mktmpdir("fiber_stream-example-") do |dir|
  input_path = File.join(dir, "input.txt")
  output_path = File.join(dir, "output.txt")

  File.write(input_path, source_text)

  chunks_written =
    Async do
      input = File.open(input_path, "rb")
      output = File.open(output_path, "wb")

      FiberStream::Source.io(input, chunk_size: 24, close: true)
        .map(&:upcase)
        .run_with(FiberStream::Sink.io(output, close: true, flush: true))
    end.wait

  puts "Wrote #{chunks_written} chunks to #{output_path}"
  puts File.read(output_path)
end
