# Ruby Ractor References

## Source

- URL: <https://docs.ruby-lang.org/en/4.0/language/ractor_md.html>
- URL: <https://docs.ruby-lang.org/en/master/Ractor.html>
- Accessed: 2026-06-02
- Observed version: Ruby 4.0 Ractor language documentation plus live Ruby
  master `Ractor` API reference
- Update policy: Re-check before implementing Ractor-backed stages or changing
  object transfer, block isolation, worker lifecycle, or error handling
  behavior.

## Summary

Ractors are Ruby's actor-like concurrency abstraction for parallel execution.
Ruby processes start with a main ractor. Threads inside one ractor share a
ractor-wide GVL, so ordinary Ruby `Thread`s in the same ractor do not execute
Ruby code in parallel with each other unless native code releases the GVL.
Different ractors can execute Ruby code in parallel on multiple cores.

Ractors isolate object access. Objects sent between ractors are handled as one
of three transfer modes:

- Shareable objects are sent by reference.
- Unshareable objects are copied by default. Deep copy can be slow and some
  objects cannot be copied.
- Unshareable objects can be moved with `move: true`. The sending ractor cannot
  use the moved object afterward.

`Ractor.shareable?` checks whether an object can be shared. `Ractor.make_shareable`
tries to make an object shareable, usually by recursively freezing it. Some
objects cannot be made shareable.

Blocks used with ractors are isolated from non-shareable outer scope.
`Ractor.shareable_proc` creates a proc usable across ractors. Local Ruby 4.0.3
spikes showed that it can capture shareable outer values such as integers or
frozen strings, but raises `Ractor::IsolationError` when the proc can refer to
an unshareable captured object such as a mutable string. This affects any
FiberStream API that asks users to provide code for a ractor worker.

Ractor ports and receive operations can block the current thread. A
FiberStream Ractor stage must not accidentally block an Async reactor thread
unless that behavior is an explicit public contract.

`Ractor.select` can wait for multiple ractors or ports. Ruby documentation notes
that using it with a very large number of ractors has a similar issue to
`select(2)`. Ruby 4.0.3 exposes `Ractor#value`, `Ractor.select`, and
`Ractor::Port`; it does not expose the older `Ractor#take` API.

Local Ruby 4.0.3 introspection on 2026-06-03 found that
`Ractor::Port#receive` accepts no arguments, while `Ractor::Port#send` accepts
`send(obj, move: ...)`. This means an ingress source cannot choose incoming
message move/copy transfer at receive time; producer-to-source transfer policy
belongs to the producer's `send` call.

Local Ruby 4.0.3 checks on 2026-06-03 also found that `Ractor::Port#close`
does not wake a different thread that is already blocked in `port.receive`.
After a port is closed, later `send` and `receive` calls raise
`Ractor::ClosedError`, but in-flight receive waits need a separate wakeup
mechanism such as `Ractor.select` over both the data port and an internal
shutdown port.

Local Ruby 4.0.3 checks also found that `Data.define` instances are shareable
when all contained values are shareable. Empty control envelopes such as
`Ack = Data.define` and `Complete = Data.define` are shareable. Envelopes
containing mutable values, such as `Element.new("x")`, are not shareable unless
the contained value is shareable or the sender uses copy/move transfer.

## Implications

- A CPU-bound FiberStream stage should use ractors rather than a plain Ruby
  thread pool when true Ruby-code parallelism is the goal.
- A Ractor-backed stage needs a separate public contract from
  `Flow.parallel_map` because object transfer, block isolation, cancellation,
  and error handling differ from scheduler-backed fiber workers.
- The first public Ractor API should require a shareable mapper proc so block
  isolation failures are explicit and early.
- Transfer policy should be visible in the API. A safe default should avoid
  moving user objects unless the user opts in.
- The design must avoid blocking scheduler-managed fibers while waiting for
  Ractor worker results.
- Ractor ingress sources should use typed, shareable control envelopes where
  possible and treat producer-to-source move/copy policy as a producer send
  responsibility.
- Ractor ingress coordinator threads should not rely on closing the data port
  to interrupt a blocking receive that is already in progress.
