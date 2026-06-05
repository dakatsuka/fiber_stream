# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "digest"
require "fiber_stream"

PRODUCER_JOBS = [
  [
    "producer-a",
    [
      { name: "alpha.bin", payload: +"A" * 180_000, seed_rounds: 80, verify_rounds: 60 },
      { name: "bravo.bin", payload: +"B" * 140_000, seed_rounds: 70, verify_rounds: 55 }
    ]
  ],
  [
    "producer-b",
    [
      { name: "charlie.bin", payload: +"C" * 220_000, seed_rounds: 85, verify_rounds: 65 },
      { name: "delta.bin", payload: +"D" * 120_000, seed_rounds: 75, verify_rounds: 50 }
    ]
  ]
].freeze

VERIFY_RECORD =
  Ractor.shareable_proc do |record|
    digest = record.fetch(:seed_sha256)

    record.fetch(:verify_rounds).times do |index|
      digest = Digest::SHA256.hexdigest("#{digest}:verify:#{index}")
    end

    record.merge(final_sha256: digest)
  end

def spawn_digest_producer(data_port, producer_name, jobs)
  setup_port = Ractor::Port.new
  producer =
    Ractor.new(data_port, setup_port, producer_name, jobs) do |outbox, setup, name, producer_jobs|
      ack_port = Ractor::Port.new
      setup.send(ack_port)

      enumerator = producer_jobs.to_enum
      sent = 0

      loop do
        case ack_port.receive
        in FiberStream::RactorPort::Ack
          begin
            job = enumerator.next
            digest = job.fetch(:payload)

            job.fetch(:seed_rounds).times do |index|
              digest = Digest::SHA256.hexdigest("#{digest}:#{name}:#{index}")
            end

            sent += 1
            outbox.send(
              FiberStream::RactorPort::Element.new(
                {
                  producer: name,
                  name: job.fetch(:name),
                  bytes: job.fetch(:payload).bytesize,
                  seed_sha256: digest,
                  verify_rounds: job.fetch(:verify_rounds)
                }
              ),
              move: true
            )
          rescue StopIteration
            outbox.send(FiberStream::RactorPort::Complete.new)
            break [:completed, name, sent]
          end
        in FiberStream::RactorPort::Cancel[reason]
          break [:cancelled, name, sent, reason]
        end
      end
    end

  [producer, setup_port.receive]
end

port_pairs = []
producers =
  PRODUCER_JOBS.map do |producer_name, jobs|
    data_port = Ractor::Port.new
    producer, ack_port = spawn_digest_producer(data_port, producer_name, jobs)
    port_pairs << { port: data_port, ack_port: ack_port }
    producer
  end

records =
  FiberStream::Source.ractor_merge_ports(port_pairs)
    .ractor_map(workers: 2, input_transfer: :move, output_transfer: :move, &VERIFY_RECORD)
    .run_with(FiberStream::Sink.to_a)

puts "Merged producer Ractors, then verified in ractor_map workers"
records.each do |record|
  puts format(
    "- %-10<producer>s %-11<name>s %7<bytes>d bytes  %<final_sha256>s",
    producer: record.fetch(:producer),
    name: record.fetch(:name),
    bytes: record.fetch(:bytes),
    final_sha256: record.fetch(:final_sha256)
  )
end

puts
puts "Producer statuses:"
producers.each do |producer|
  puts "- #{producer.value.inspect}"
end

puts
puts "Source.ractor_merge_ports emits producers in ready order."
puts "ractor_map preserves that merged input order while running verification in Ractor workers."
