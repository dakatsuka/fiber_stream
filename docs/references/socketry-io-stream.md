# Socketry IO::Stream Reference

## Source

- URL: <https://github.com/socketry/io-stream>
- URL: <https://socketry.github.io/io-stream/>
- URL: <https://socketry.github.io/io-stream/releases/>
- Accessed: 2026-05-31
- Observed version: io-stream 0.13.0 release notes and current repository
  README
- Update policy: Re-check before adding direct `io-stream` integration tests,
  examples, or optional dependency behavior.

## Summary

`io-stream` provides a buffered stream implementation for Ruby that is
independent of the underlying IO object. Its stated motivation is to normalize
differences between Ruby IO types, including buffering behavior,
`OpenSSL::SSL::SSLSocket`, EOF semantics, connection reset errors, close
behavior, and non-blocking IO edge cases.

As of the 2026-05-31 check, `io-stream` is a gem in the Socketry ecosystem, not
part of Ruby core. Ruby 4.0 documents `IO` itself as the core basis for input
and output, with core and standard-library objects such as `File`, pipes,
sockets, and standard streams exposing the methods FiberStream needs. Ruby's
`IO::Stream` proposal remains tracked separately from the current Ruby core IO
API.

The public surface includes `IO.Stream(...)` / `IO::Stream::Duplex(...)`
wrappers and readable/writable methods such as `read`, `read_partial`,
`readpartial`, `write`, `flush`, and close operations. Release notes describe
support for `read_partial` buffers, EOF compatibility, `gets`, connection reset
normalization, and duplex stream composition.

## Implications

FiberStream should treat Ruby core IO objects as the primary integration path
and `io-stream` as a compatible low-level IO adapter, not as a competing
stream-processing runtime.

- FiberStream remains responsible for `Source`, `Flow`, `Sink`, pull
  backpressure, async/buffer boundaries, and materialization.
- Ruby core IO objects such as `File`, `IO.pipe` endpoints, sockets, `$stdin`,
  `$stdout`, and `$stderr` should be the default examples and primary behavior
  target.
- `io-stream` remains responsible for IO buffering and normalization across
  specific Ruby IO implementations.
- FiberStream should not add `io-stream` as a runtime dependency for the first
  IO source or sink APIs.
- FiberStream APIs should accept caller-provided `IO::Stream` wrappers when
  they implement the structural methods FiberStream needs, such as
  `readpartial`, `write`, `flush`, and `close`.
- If future docs show `io-stream` examples, make the dependency explicit in the
  example rather than implying FiberStream loads it.
