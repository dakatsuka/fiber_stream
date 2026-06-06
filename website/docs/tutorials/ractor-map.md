# Ractor Map

Use `ractor_map` for ordered CPU-bound mapping in worker Ractors.

The mapper must be shareable. A common approach is `Ractor.shareable_proc`.

```ruby
require "digest"
require "fiber_stream"

records = [
  { name: "alpha.bin", payload: +"A" * 200_000 },
  { name: "bravo.bin", payload: +"B" * 120_000 }
]

hash_record =
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
    .ractor_map(workers: 2, input_transfer: :move, &hash_record)
    .run_with(FiberStream::Sink.to_a)
```

Results are emitted in input order. At most `workers` upstream elements are
pulled but not yet emitted.

Use `input_transfer: :move` only when the sender will not use the moved object
again.

The runnable version is `examples/ractor_map_hashing.rb`.
