# frozen_string_literal: true

module FiberStream
  class Flow
    # Creates a mapping flow.
    #
    # The block is called once for each element pulled through this flow.
    # Exceptions raised by the block fail the stream and are re-raised from
    # `Source#run_with`.
    def self.map(&block)
      raise ArgumentError, "missing block" unless block

      new { |upstream| Pull.map(upstream, block) }
    end

    # Creates a filtering flow.
    #
    # The block is called for upstream elements until it returns a truthy value
    # or upstream completes. Matching elements pass through unchanged.
    # Exceptions raised by the block fail the stream and are re-raised from
    # `Source#run_with`.
    def self.select(&block)
      raise ArgumentError, "missing block" unless block

      new { |upstream| Pull.select(upstream, block) }
    end

    # Creates a limiting flow.
    #
    # The flow emits at most `count` elements. `take(0)` completes without
    # pulling upstream and closes upstream on the first downstream demand. After
    # the limit is reached, upstream is closed during the pull that forwards
    # the final element. Negative counts raise `ArgumentError`; non-Integer
    # counts raise `TypeError`.
    def self.take(count)
      raise TypeError, "count must be an Integer" unless count.is_a?(Integer)
      raise ArgumentError, "count must be non-negative" if count.negative?

      new { |upstream| Pull.take(upstream, count) }
    end

    # Creates a scheduler-backed asynchronous boundary.
    #
    # The boundary starts its producer on the first downstream demand and
    # requires an installed `Fiber.scheduler` at that point. Upstream stages run
    # in a non-blocking producer fiber, downstream stages remain in the caller's
    # current fiber, and each downstream pull resumes at most one upstream pull.
    # Closing the boundary closes upstream and requests producer cancellation.
    # FiberStream does not depend on Async at runtime.
    def self.async
      new { |upstream| Pull.async(upstream) }
    end

    # Creates a bounded asynchronous buffer.
    #
    # The buffer starts its producer on the first downstream demand and requires
    # an installed `Fiber.scheduler` at that point. It preserves element order,
    # stores at most `count` messages, and closes upstream while requesting
    # producer cancellation when closed. `count` must be a positive Integer.
    # FiberStream does not depend on Async at runtime.
    def self.buffer(count)
      raise TypeError, "count must be an Integer" unless count.is_a?(Integer)
      raise ArgumentError, "count must be positive" unless count.positive?

      new { |upstream| Pull.buffer(upstream, count) }
    end

    # Creates a line-splitting flow.
    #
    # The flow accepts String chunks and emits lines split on "\n". By default
    # it chomps the trailing newline and one preceding "\r". `max_length` is an
    # optional per-line bytesize limit.
    def self.lines(chomp: true, max_length: nil)
      raise TypeError, "chomp must be true or false" unless [true, false].include?(chomp)
      unless max_length.nil? || max_length.is_a?(Integer)
        raise TypeError, "max_length must be nil or an Integer"
      end
      raise ArgumentError, "max_length must be positive" if max_length&.<= 0

      new { |upstream| Pull.lines(upstream, chomp, max_length) }
    end

    # Returns a reusable flow that applies this flow and then `flow`.
    #
    # Construction is lazy. No upstream stream is attached and no elements are
    # pulled until the composed flow is materialized by a source or sink.
    def via(flow)
      raise TypeError, "expected FiberStream::Flow" unless flow.is_a?(Flow)

      self.class.__send__(:new) do |upstream|
        attached_stream = attach(upstream)

        begin
          flow.__send__(:attach, attached_stream)
        rescue StandardError
          begin
            attached_stream.close
          rescue StandardError
            nil
          end
          raise
        end
      end
    end

    # Returns a sink that runs this flow before `sink`.
    #
    # The composed sink accepts this flow's input elements and returns the
    # wrapped sink's materialized value. It closes the attached flow chain after
    # normal completion, failure, or early sink completion.
    def to(sink)
      raise TypeError, "expected FiberStream::Sink" unless sink.is_a?(Sink)

      Sink.__send__(:new) do |stream|
        attached_stream = nil
        primary_error = nil

        begin
          attached_stream = attach(stream)
          sink.__send__(:run, attached_stream)
        rescue StandardError => error
          primary_error = error
          raise
        ensure
          begin
            attached_stream&.close
          rescue StandardError => close_error
            raise close_error unless primary_error
          end
        end
      end
    end

    def initialize(&attach)
      @attach = attach
    end

    private_class_method :new

    private

    def attach(upstream)
      @attach.call(upstream)
    end
  end
end
