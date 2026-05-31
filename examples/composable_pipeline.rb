# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "fiber_stream"

input_lines = [
  "  id,status,total ",
  " 1001,active,12000 ",
  "",
  " 1002,cancelled,3000 ",
  " 1003,active,25000 "
]

normalize_lines =
  FiberStream::Flow.map(&:strip)
    .via(FiberStream::Flow.select { |line| !line.empty? })

parse_order =
  FiberStream::Flow.map do |line|
    id, status, total = line.split(",", 3)

    {
      id: id,
      status: status,
      total: Integer(total)
    }
  end

active_order_sink =
  parse_order
    .via(FiberStream::Flow.select { |order| order.fetch(:status) == "active" })
    .to(FiberStream::Sink.to_a)

pipeline =
  FiberStream::Source.each(input_lines.drop(1))
    .via(normalize_lines)
    .to(active_order_sink)

puts "Active orders"
pipeline.run.each do |order|
  puts "- ##{order.fetch(:id)} JPY #{order.fetch(:total)}"
end
