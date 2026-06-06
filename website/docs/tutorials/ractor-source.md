<script setup>
import RactorSourceSequence from "../.vitepress/theme/components/RactorSourceSequence.vue";
</script>

# Ractor Source

Use `Source.ractor_port` when a producer Ractor should become a FiberStream
source.

The producer owns an acknowledgement port. It waits for
`RactorPort::Ack`, sends one stream message, then waits for the next ack.
This keeps producer output tied to downstream demand.

<RactorSourceSequence />

## Single producer

```ruby
require "fiber_stream"

data_port = Ractor::Port.new
setup_port = Ractor::Port.new

producer =
  Ractor.new(data_port, setup_port) do |outbox, setup|
    ack_port = Ractor::Port.new
    setup.send(ack_port)

    values = (1..5).to_enum

    loop do
      case ack_port.receive
      in FiberStream::RactorPort::Ack
        begin
          outbox.send(FiberStream::RactorPort::Element.new(values.next))
        rescue StopIteration
          outbox.send(FiberStream::RactorPort::Complete.new)
          break
        end
      in FiberStream::RactorPort::Cancel
        break
      end
    end
  end

ack_port = setup_port.receive

result =
  FiberStream::Source.ractor_port(data_port, ack_port: ack_port)
    .map { |number| number * number }
    .run_with(FiberStream::Sink.to_a)

result # => [1, 4, 9, 16, 25]
producer.value
```

`Source.ractor_port` does not require a `Fiber.scheduler`. Producer work runs
inside the Ractor. The stream still remains demand-driven because the producer
sends only after receiving an ack.

Values sent through the data port must obey Ruby Ractor transfer rules. Use
`move: true` only when the producer will not reuse the moved object.

## Failure messages

A producer can fail the stream by sending `RactorPort::Failure`.

```ruby
outbox.send(
  FiberStream::RactorPort::Failure.new(
    "ProducerError",
    "failed to build next value"
  )
)
break
```

`RactorPort::Complete` and `RactorPort::Failure` are terminal producer
messages.

Failure metadata is producer-provided. Redact paths, secrets, tenant data, or
other sensitive values before sending failures across trust boundaries.

## Multiple producers

Use `Source.ractor_merge_ports` when several producer Ractors should feed one
source.

```ruby
port_pairs = [
  { port: data_port_a, ack_port: ack_port_a },
  { port: data_port_b, ack_port: ack_port_b }
]

records =
  FiberStream::Source.ractor_merge_ports(port_pairs)
    .run_with(FiberStream::Sink.to_a)
```

Each producer receives at most one outstanding ack. Values are emitted in
coordinator-observed ready order. Each producer's own order is preserved.

The runnable examples are `examples/ractor_port_source.rb` and
`examples/ractor_merge_ports_and_map.rb`.
