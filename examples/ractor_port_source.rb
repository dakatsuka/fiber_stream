# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "fiber_stream"

data_port = Ractor::Port.new
setup_port = Ractor::Port.new

producer =
  Ractor.new(data_port, setup_port) do |outbox, setup|
    ack_port = Ractor::Port.new
    setup.send(ack_port)

    values = (1..5).to_enum
    sent = 0

    loop do
      case ack_port.receive
      in FiberStream::RactorPort::Ack
        begin
          value = values.next
          sent += 1
          outbox.send(FiberStream::RactorPort::Element.new(value))
        rescue StopIteration
          outbox.send(FiberStream::RactorPort::Complete.new)
          break [:completed, sent]
        end
      in FiberStream::RactorPort::Cancel[reason]
        break [:cancelled, sent, reason]
      end
    end
  end

ack_port = setup_port.receive

result =
  FiberStream::Source.ractor_port(data_port, ack_port: ack_port)
    .map { |number| number * number }
    .run_with(FiberStream::Sink.to_a)

puts "Squares from a producer Ractor:"
puts result.join(", ")
puts
puts "Producer status: #{producer.value.inspect}"
