# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "fiber_stream"

orders = [
  { id: 101, customer: "Aki", total: 4_800 },
  { id: 102, customer: "Mina", total: 12_400 },
  { id: 103, customer: "Ren", total: 9_900 },
  { id: 104, customer: "Sora", total: 18_200 }
]

high_value_summaries =
  FiberStream::Source.each(orders)
    .select { |order| order.fetch(:total) >= 10_000 }
    .map do |order|
      format(
        "#%<id>d %-4<customer>s JPY %<total>d",
        id: order.fetch(:id),
        customer: order.fetch(:customer),
        total: order.fetch(:total)
      )
    end
    .run_with(FiberStream::Sink.to_a)

puts "High-value orders"
high_value_summaries.each { |summary| puts "- #{summary}" }
