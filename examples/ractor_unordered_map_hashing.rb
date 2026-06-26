# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "digest"
require "fiber_stream"

records = [
  { name: "slow-alpha.bin", payload: +"A" * 160_000, rounds: 1_200 },
  { name: "fast-bravo.bin", payload: +"B" * 120_000, rounds: 120 },
  { name: "fast-charlie.bin", payload: +"C" * 140_000, rounds: 120 },
  { name: "medium-delta.bin", payload: +"D" * 100_000, rounds: 450 }
]

HASH_RECORD =
  Ractor.shareable_proc do |record|
    payload = record.fetch(:payload)
    digest = Digest::SHA256.hexdigest(payload)

    record.fetch(:rounds).times do
      digest = Digest::SHA256.hexdigest(digest)
    end

    {
      name: record.fetch(:name),
      bytes: payload.bytesize,
      rounds: record.fetch(:rounds),
      sha256: digest
    }
  end

puts "Input order"
records.each.with_index(1) do |record, index|
  puts format(
    "%<index>2d. %-16<name>s %4<rounds>d rounds",
    index: index,
    name: record.fetch(:name),
    rounds: record.fetch(:rounds)
  )
end

digests =
  FiberStream::Source.each(records)
    .ractor_unordered_map(workers: 2, input_transfer: :move, &HASH_RECORD)
    .run_with(FiberStream::Sink.to_a)

puts
puts "Completion order"
digests.each.with_index(1) do |digest, index|
  puts format(
    "%<index>2d. %-16<name>s %7<bytes>d bytes %4<rounds>d rounds  %<sha256>s",
    index: index,
    name: digest.fetch(:name),
    bytes: digest.fetch(:bytes),
    rounds: digest.fetch(:rounds),
    sha256: digest.fetch(:sha256)
  )
end

puts
puts "Results are emitted as Ractor workers finish, not by input position."
puts "input_transfer: :move is safe here because the input records are not reused."
