# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "digest"
require "fiber_stream"

records = [
  { name: "alpha.bin", payload: +"A" * 200_000 },
  { name: "bravo.bin", payload: +"B" * 120_000 },
  { name: "charlie.bin", payload: +"C" * 260_000 },
  { name: "delta.bin", payload: +"D" * 80_000 }
]

HASH_RECORD =
  Ractor.shareable_proc do |record|
    payload = record.fetch(:payload)

    {
      name: record.fetch(:name),
      bytes: payload.bytesize,
      sha256: Digest::SHA256.hexdigest(payload)
    }
  end

digests =
  FiberStream::Source.each(records)
    .ractor_map(workers: 2, input_transfer: :move, &HASH_RECORD)
    .run_with(FiberStream::Sink.to_a)

puts "SHA-256 digests"
digests.each do |digest|
  puts format(
    "- %-11<name>s %7<bytes>d bytes  %<sha256>s",
    name: digest.fetch(:name),
    bytes: digest.fetch(:bytes),
    sha256: digest.fetch(:sha256)
  )
end

puts
puts "Results are emitted in input order."
puts "input_transfer: :move is safe here because the input records are not reused."
