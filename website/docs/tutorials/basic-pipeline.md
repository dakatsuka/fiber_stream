# Basic Pipeline

This tutorial filters in-memory records and collects formatted strings.

```ruby
require "fiber_stream"

orders = [
  { id: 101, customer: "Aki", total: 4_800 },
  { id: 102, customer: "Mina", total: 12_400 },
  { id: 103, customer: "Ren", total: 9_900 },
  { id: 104, customer: "Sora", total: 18_200 }
]

summaries =
  FiberStream::Source.each(orders)
    .select { |order| order.fetch(:total) >= 10_000 }
    .map do |order|
      format(
        "#%<id>d %<customer>s JPY %<total>d",
        id: order.fetch(:id),
        customer: order.fetch(:customer),
        total: order.fetch(:total)
      )
    end
    .run_with(FiberStream::Sink.to_a)

summaries
# => ["#102 Mina JPY 12400", "#104 Sora JPY 18200"]
```

The source is not consumed when the chain is built. Work starts at
`run_with`.

The same example is available as `examples/basic_pipeline.rb`.
