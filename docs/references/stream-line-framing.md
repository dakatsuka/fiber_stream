# Stream Line Framing References

## Source

- Akka Streams `Framing.delimiter`:
  https://doc.akka.io/api/akka-core/current/akka/stream/scaladsl/Framing%24.html
- ZIO Streams `ZPipeline.splitLines`:
  https://zio.dev/reference/stream/zpipeline
- FS2 `text.lines`:
  https://www.javadoc.io/static/co.fs2/fs2-docs_3/3.8-a43eaac/fs2/text%24.html
- Ruby core `IO#each_line`:
  https://ruby-doc.org/core/IO.html
- Accessed: 2026-05-31

## Summary

Stream libraries commonly provide a stage that turns unstructured byte or text
chunks into record-like lines or frames.

Akka Streams exposes delimiter framing as a general byte-oriented flow and
includes maximum-frame-length protection. ZIO Streams and FS2 expose newline
splitting as reusable pipeline/pipe stages. Ruby core IO exposes line
iteration directly through `IO#each_line`.

## Implications

FiberStream should provide a discoverable `Flow.lines` API for the common
newline case. The first implementation should remain narrower than Akka's
general delimiter framing, but it should include a `max_length:` option so
users can protect long-running streams from unbounded memory growth when a
delimiter never arrives.

If a general delimiter stage is added later, `Flow.lines` should remain as the
ergonomic newline alias.
