# frozen_string_literal: true

module FiberStream
  class SchedulerRequiredError < RuntimeError; end
  class FrameTooLongError < RuntimeError; end
  class PipelineCancelledError < RuntimeError; end

  # Normalized failure raised by `Source.ractor_port`.
  #
  # Producer failures, invalid protocol messages, and source-side Ractor port
  # failures use this stable error shape so callers do not need to depend on
  # Ruby's Ractor transport exceptions. For producer failures,
  # `cause_class_name` and `cause_message` come from the producer's
  # `RactorPort::Failure` envelope and are included in this error's public
  # message.
  class RactorPortSourceError < RuntimeError
    attr_reader :kind, :cause_class_name, :cause_message, :original_cause

    def initialize(kind:, cause_class_name:, cause_message:, cause: nil)
      @kind = kind
      @cause_class_name = cause_class_name
      @cause_message = cause_message
      @original_cause = cause

      super("ractor_port #{kind} failure: #{cause_class_name}: #{cause_message}")
    end
  end

  # Normalized failure raised for Ractor-backed mapping errors.
  #
  # Worker exceptions and Ractor transfer failures may not be directly
  # transferable back to the main ractor. This error preserves the ordered input
  # sequence, failure kind, and original exception class/message metadata.
  class RactorMapError < RuntimeError
    attr_reader :sequence, :kind, :cause_class_name, :cause_message, :original_cause

    def initialize(sequence:, kind:, cause_class_name:, cause_message:, cause: nil)
      @sequence = sequence
      @kind = kind
      @cause_class_name = cause_class_name
      @cause_message = cause_message
      @original_cause = cause

      super("ractor_map #{kind} failure at sequence #{sequence}: #{cause_class_name}: #{cause_message}")
    end
  end
end
