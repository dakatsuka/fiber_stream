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

    private_class_method :new
  end
end
