# frozen_string_literal: true

module FiberStream
  class Pipeline
    def initialize(source, sink)
      @source = source
      @sink = sink
    end

    # Runs this pipeline in the current fiber.
    #
    # This is equivalent to `source.run_with(sink)` for the source and sink
    # definitions captured by `Source#to`. Repeated runs create new
    # materializations, subject to the replayability and resource ownership
    # semantics of the captured endpoints.
    def run
      @source.run_with(@sink)
    end

    # Runs this pipeline in a scheduler-backed background fiber.
    #
    # The method starts one new materialization and returns a `RunningPipeline`
    # handle that can wait for the materialized value, observe completion, and
    # request cancellation. Starting background execution requires an installed
    # `Fiber.scheduler` from a non-blocking current fiber. FiberStream does not
    # depend on Async at runtime.
    def run_async
      validate_scheduler!

      RunningPipeline.__send__(:new, Fiber.scheduler) { run }
    end

    private_class_method :new

    private

    def validate_scheduler!
      return if Fiber.scheduler && !Fiber.current.blocking?

      message =
        if Fiber.scheduler
          "Pipeline#run_async requires a non-blocking fiber"
        else
          "Pipeline#run_async requires Fiber.scheduler"
        end
      raise SchedulerRequiredError, message
    end
  end
end
