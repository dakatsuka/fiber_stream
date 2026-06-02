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

Blocks used with ractors are isolated from outer scope. `Ractor.shareable_proc`
creates a proc usable across ractors, but it cannot capture outer variables.
This affects any FiberStream API that asks users to provide code for a ractor
worker.

Ractor ports and receive operations can block the current thread. A
FiberStream Ractor stage must not accidentally block an Async reactor thread
unless that behavior is an explicit public contract.

`Ractor.select` can wait for multiple ractors or ports. Ruby documentation notes
that using it with a very large number of ractors has a similar issue to
`select(2)`.

## Implications

- A CPU-bound FiberStream stage should use ractors rather than a plain Ruby
  thread pool when true Ruby-code parallelism is the goal.
- A Ractor-backed stage needs a separate public contract from
  `Flow.parallel_map` because object transfer, block isolation, cancellation,
  and error handling differ from scheduler-backed fiber workers.
- The first public Ractor API should require a `Ractor.shareable_proc` block so
  block isolation failures are explicit and early.
- Transfer policy should be visible in the API. A safe default should avoid
  moving user objects unless the user opts in.
- The design must decide how to avoid blocking scheduler-managed fibers while
  waiting for Ractor worker results.
