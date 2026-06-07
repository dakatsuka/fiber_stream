# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "fiber_stream"

EMIT_NUMBERS =
  Ractor.shareable_proc do |producer, range|
    range.each do |number|
      break unless producer.emit(number)
    end
  end

EMIT_TAGGED_NUMBERS =
  Ractor.shareable_proc do |producer, tag, range|
    range.each do |number|
      break unless producer.emit([tag, number])
    end
  end

squares =
  FiberStream::Source.ractor_producer(1..5, &EMIT_NUMBERS)
    .map { |number| number * number }
    .run_with(FiberStream::Sink.to_a)

puts "Squares from one FiberStream-owned producer Ractor:"
puts squares.join(", ")

merged =
  FiberStream::Source.ractor_merge_producers do |group|
    group.producer(:low, 1..3, &EMIT_TAGGED_NUMBERS)
    group.producer(:high, 10..12, &EMIT_TAGGED_NUMBERS)
  end.run_with(FiberStream::Sink.to_a)

puts
puts "Merged values from two owned producer Ractors:"
merged.each do |tag, number|
  puts "- #{tag}: #{number}"
end

puts
puts "Producer blocks use RactorProducer#emit and stop when it returns false."
puts "FiberStream creates the data ports, ack ports, producer Ractors, and cleanup path."
