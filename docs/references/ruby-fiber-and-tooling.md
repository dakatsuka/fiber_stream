# Ruby Fiber And Tooling References

## Status

Active

## Ruby Fiber Scheduler

Source:

- URL: <https://docs.ruby-lang.org/en/4.0/fiber_md.html>
- Accessed: 2026-05-31
- Observed version: Ruby 4.0 documentation
- Update policy: Re-check before implementing scheduler-dependent IO,
  `.async`, or `.buffer` behavior.

Ruby's Fiber scheduler interface separates application code from event loop
implementations. A scheduler can intercept blocking operations and redirect
them to an event loop. FiberStream should treat this as an environmental
capability and should not install a scheduler itself.

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
