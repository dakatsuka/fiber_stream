<script setup>
import RactorSourceSequence from "../.vitepress/theme/components/RactorSourceSequence.vue";
</script>

# Ractor Source

Use `Source.ractor_producer` when FiberStream should own a producer Ractor and
turn its output into a source.

The producer block receives a `RactorProducer` context. Each `emit` call waits
for downstream demand before sending one value, so producer output remains
tied to stream demand.

<RactorSourceSequence />

## Single producer

```ruby
require "fiber_stream"

produce_values =
  Ractor.shareable_proc do |producer, values|
    values.each do |value|
      break unless producer.emit(value)
    end
  end

result =
  FiberStream::Source.ractor_producer(1..5, &produce_values)
    .map { |number| number * number }
    .run_with(FiberStream::Sink.to_a)

result # => [1, 4, 9, 16, 25]
```

`Source.ractor_producer` does not require a `Fiber.scheduler`. Producer work
runs inside the Ractor, and FiberStream creates the ports, acknowledgment
messages, and cooperative cleanup path.

Values emitted by the producer must obey Ruby Ractor transfer rules. Use
`transfer: :move` only when the producer will not reuse the moved object.

## Producer failure

A producer can fail the stream explicitly with `producer.fail`.

```ruby
producer.fail(cause_class_name: "ProducerError", cause_message: "failed")
```

Failure metadata is producer-provided. Redact paths, secrets, tenant data, or
other sensitive values before sending failures across trust boundaries.

## Multiple producers

Use `Source.ractor_merge_producers` when several FiberStream-owned producer
Ractors should feed one source.

```ruby
produce_tagged_values =
  Ractor.shareable_proc do |producer, tag, values|
    values.each do |value|
      break unless producer.emit([tag, value])
    end
  end

records =
  FiberStream::Source
    .ractor_merge_producers do |group|
      group.producer(:a, [1, 2], &produce_tagged_values)
      group.producer(:b, [3, 4], &produce_tagged_values)
    end
    .run_with(FiberStream::Sink.to_a)

# Example result: [[:a, 1], [:b, 3], [:a, 2], [:b, 4]]
```

Each producer receives at most one outstanding ack. Values are emitted in
coordinator-observed ready order. Each producer's own order is preserved.

## Externally owned producers

Use `Source.ractor_port` and `Source.ractor_merge_ports` when producer Ractors
are owned outside FiberStream or need custom lifecycle handling. The producer
owns an acknowledgment port, waits for `RactorPort::Ack`, sends one
`RactorPort::Element`, `RactorPort::Complete`, or `RactorPort::Failure`, then
waits for the next ack.

The runnable examples are `examples/ractor_producer_sources.rb`,
`examples/ractor_port_source.rb`, and `examples/ractor_merge_ports_and_map.rb`.
