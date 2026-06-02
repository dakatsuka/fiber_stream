# frozen_string_literal: true

module FiberStream
  class SchedulerRequiredError < RuntimeError; end
  class FrameTooLongError < RuntimeError; end
  class PipelineCancelledError < RuntimeError; end
end
