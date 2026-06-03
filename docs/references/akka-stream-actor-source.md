# Akka Streams Actor Source References

## Source

- URL: <https://doc.akka.io/libraries/akka-core/current/stream/operators/Source/actorRef.html>
- URL: <https://doc.akka.io/libraries/akka-core/current/stream/operators/Source/actorRefWithBackpressure.html>
- Accessed: 2026-06-03
- Observed version: Akka core current stream operator documentation
- Update policy: Re-check before implementing actor/Ractor ingress sources,
  changing handshake semantics, or adding one-way buffered source APIs.

## Summary

Akka Streams separates actor ingress into distinct operators. `Source.actorRef`
materializes an actor reference that accepts pushed messages and emits them
when downstream demand and buffered messages are available. Because the
communication is one-way, it does not provide backpressure to the producer.
Overflow is handled through an explicit buffer size and overflow strategy.

Akka also provides a separate backpressure-aware actor ingress operator,
`Source.actorRefWithBackpressure`, instead of overloading the one-way operator.
That split is important for FiberStream because backpressure is a core design
property, not an optional stage detail.

## Implications

- FiberStream should not overload `Source.each` to accept Ractor ingress.
- FiberStream should not make a one-way push Ractor source the first Ractor
  source API.
- The first Ractor ingress API should require an explicit handshake protocol so
  producer progress is tied to downstream demand.
- A later buffered one-way Ractor source, if added, should be a separate API
  with explicit buffer and overflow contracts.
