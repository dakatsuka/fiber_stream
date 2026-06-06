# Errors

FiberStream raises ordinary Ruby exceptions from failed stages and normalized
library errors at concurrency and protocol boundaries.

## SchedulerRequiredError

Raised when a scheduler-backed API is demanded or started without an installed
`Fiber.scheduler` and a non-blocking current fiber.

Affected APIs include:

- `Source.io`
- `Source#merge`
- `Flow.async`
- `Flow.buffer`
- `Flow.parallel_map`
- `Sink.io`
- `Pipeline#run_async`

## FrameTooLongError

Raised by `Flow.lines` or `Flow.split` when a frame exceeds `max_length`.

Set `max_length` for untrusted or unbounded input streams.

## PipelineCancelledError

Raised by `RunningPipeline#wait` when a cancellation request interrupts
background materialization.

## RactorPortSourceError

Raised by `Source.ractor_port` and `Source.ractor_merge_ports` for normalized
Ractor port failures.

Public readers:

- `kind`
- `cause_class_name`
- `cause_message`

Producer failure metadata is producer-provided and may contain application
data. Redact it before sending failures across trust boundaries.

## RactorMapError

Raised by `Flow.ractor_map` for worker failures, transfer failures, worker
termination, or isolation errors.

Public readers:

- `sequence`
- `kind`
- `cause_class_name`
- `cause_message`
