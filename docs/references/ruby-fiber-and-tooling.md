# Ruby Fiber And Tooling References

## Status

Active

## Ruby Fiber Scheduler

Source:

- URL: <https://docs.ruby-lang.org/en/4.0/fiber_md.html>
- URL: <https://docs.ruby-lang.org/en/master/Fiber/Scheduler.html>
- Accessed: 2026-05-31
- Observed version: Ruby 4.0 documentation plus live Ruby master
  `Fiber::Scheduler` API reference
- Update policy: Re-check before implementing scheduler-dependent IO,
  `.async`, or `.buffer` behavior.

Ruby's Fiber scheduler interface separates application code from event loop
implementations. A scheduler can intercept blocking operations and redirect
them to an event loop. FiberStream should treat this as an environmental
capability and should not install a scheduler itself.

The scheduler docs describe non-blocking fibers as calling scheduler hooks when
they reach blocking operations such as sleep, process waits, or non-ready IO.
Scheduler implementations register the wait, yield to other fibers, and later
resume the waiting fiber. Ruby also documents scheduler hooks for IO close,
fiber interruption, scheduler yield, and blocking operation waits. Those hooks
matter for future cancellation and resource-owning IO stages.

Ruby 4.0.3 local behavior checked on 2026-05-31: `Fiber.schedule` raises
`RuntimeError` with message `No scheduler is available!` when no scheduler is
installed. FiberStream APIs that require scheduled fibers should expose a
FiberStream-specific error instead of leaking that runtime message.

Ruby IO scheduler references re-checked on 2026-05-31 before IO source design:
the fiber documentation states that scheduler hooks run only in non-blocking
execution contexts and that IO operations invoke the scheduler when a scheduler
exists and the current thread is in non-blocking execution. It also documents
that closing an IO interrupts blocking operations on that IO and removes
blocked fibers or threads before the descriptor is closed. The live
`Fiber::Scheduler` API lists IO hooks including `io_wait`, `io_read`,
`io_write`, `io_select`, `io_close`, and `fiber_interrupt`.

Ruby 4.0 IO documentation for `IO#readpartial(maxlen)` says it reads up to
`maxlen` bytes, returns a new `String` with ASCII-8BIT encoding when no
`out_string` is supplied, blocks only when no buffered or stream data is
available and EOF has not been reached, and raises `EOFError` at EOF.

## Async

Sources:

- URL: <https://socketry.github.io/async/>
- URL: <https://socketry.github.io/async/guides/getting-started/>
- Accessed: 2026-05-31
- Observed version: Async 2.39.0 in `Gemfile.lock`
- Update policy: Re-check before adding Async compatibility tests or changing
  the development dependency.

Async is a fiber-based asynchronous IO framework for Ruby. It provides a
scheduler and task model, and is a good compatibility target for FiberStream's
future async and IO behavior. FiberStream should not depend on Async at runtime.

Async tasks run on fibers. Blocking operations such as sleep, reads, and writes
yield control until they complete, allowing other fibers to run. The top-level
`Async { ... }` block creates a reactor and scheduler. `Sync { ... }` runs in
the current event loop when one exists, or creates one when needed. Async's
scheduler can also be installed directly with `Fiber.set_scheduler`.

Async scheduler behavior should not be treated as a deterministic ordering
contract. Its docs distinguish optimistic and pessimistic scheduling strategies
and warn users not to rely on exact execution order.

## RBS

Source:

- URL: <https://github.com/ruby/rbs>
- Accessed: 2026-05-31
- Observed version: RBS 4.0.2 in `Gemfile.lock`
- Update policy: Re-check before changing public API signature style.

RBS describes the structure of Ruby programs through external type signatures.
FiberStream should provide RBS for public APIs from the first implementation.

## RuboCop

Source:

- URL: <https://docs.rubocop.org/rubocop/latest/configuration/cop_settings.html>
- Accessed: 2026-05-31
- Observed version: RuboCop 1.87.0 in `Gemfile.lock`
- Update policy: Re-check before broadening enabled cops.

RuboCop supports enabling only selected departments. FiberStream uses a minimal
configuration with Layout and Lint enabled, while style and metrics rules remain
disabled until the project needs stricter policy.

## GitHub Actions

Sources:

- URL: <https://github.com/ruby/setup-ruby>
- URL: <https://docs.github.com/en/actions/tutorials/build-and-test-code/ruby>
- Accessed: 2026-05-31
- Observed version: `ruby/setup-ruby@v1`
- Update policy: Re-check before changing CI Ruby versions or dependency cache
  behavior.

The Ruby setup action supports Bundler caching. FiberStream CI uses
`ruby/setup-ruby@v1` with Ruby 4.0.3 and `bundler-cache: true`.
