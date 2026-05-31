# frozen_string_literal: true

module FiberStream
  class Source
    # Creates a source definition from an Enumerable.
    #
    # The enumerable is not consumed until values are pulled by `run_with`. Each
    # materialization creates an Enumerator with `enumerable.to_enum(:each)`;
    # FiberStream does not snapshot values or guarantee replayability for
    # one-shot enumerables.
    def self.each(enumerable)
      new(-> { Pull.each(enumerable) })
    end

    # Creates a source definition from an IO-like object.
    #
    # The IO object is not read until values are pulled by `run_with`. Each
    # materialization reads from the same IO object's current position; this
    # source does not snapshot, reopen, or guarantee replayability. The IO is
    # closed only when `close: true` is passed.
    def self.io(io, chunk_size: 16 * 1024, close: false)
      raise TypeError, "io must respond to readpartial" unless io.respond_to?(:readpartial)
      raise TypeError, "chunk_size must be an Integer" unless chunk_size.is_a?(Integer)
      raise ArgumentError, "chunk_size must be positive" unless chunk_size.positive?
      raise TypeError, "close must be true or false" unless [true, false].include?(close)
      raise TypeError, "io must respond to close" if close && !io.respond_to?(:close)

      new(-> { Pull.io(io, chunk_size, close) })
    end

    def initialize(source_factory, flows = [])
      @source_factory = source_factory
      @flows = flows
    end

    # Returns a new source definition that passes this source through `flow`.
    #
    # This method is lazy. It does not run the source, enumerate values, or call
    # flow blocks.
    def via(flow)
      raise TypeError, "expected FiberStream::Flow" unless flow.is_a?(Flow)

      self.class.__send__(:new, @source_factory, @flows + [flow])
    end

    # Returns a new source definition that maps each element with `block`.
    #
    # This is a convenience wrapper around `via(FiberStream::Flow.map { ... })`
    # and has the same lazy construction, error, and backpressure behavior as
    # the underlying flow.
    def map(&block)
      via(Flow.map(&block))
    end

    # Returns a new source definition that keeps elements matching `block`.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.select { ... })` and has the same truthiness and
    # lazy construction behavior as the underlying flow.
    def select(&block)
      via(Flow.select(&block))
    end

    # Returns a new source definition that emits at most `count` elements.
    #
    # This is a convenience wrapper around `via(FiberStream::Flow.take(count))`
    # and preserves the same validation and upstream close behavior.
    def take(count)
      via(Flow.take(count))
    end

    # Returns a new source definition with an asynchronous boundary.
    #
    # This is a convenience wrapper around `via(FiberStream::Flow.async)` and
    # preserves the same scheduler requirement and cancellation behavior.
    def async
      via(Flow.async)
    end

    # Returns a new source definition with a bounded asynchronous buffer.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.buffer(count))` and preserves the same validation,
    # scheduler requirement, and cancellation behavior.
    def buffer(count)
      via(Flow.buffer(count))
    end

    # Materializes and runs this source with `sink`.
    #
    # The stream runs in the current fiber until completion or failure. The
    # method returns the sink's materialized value and closes the materialized
    # pull chain on success, failure, or early sink completion.
    def run_with(sink)
      raise TypeError, "expected FiberStream::Sink" unless sink.is_a?(Sink)

      primary_error = nil

      begin
        stream = @source_factory.call
        @flows.each do |flow|
          stream = flow.__send__(:attach, stream)
        end

        sink.__send__(:run, stream)
      rescue StandardError => error
        primary_error = error
        raise
      ensure
        begin
          stream&.close
        rescue StandardError => close_error
          raise close_error unless primary_error
        end
      end
    end

    private_class_method :new
  end
end
