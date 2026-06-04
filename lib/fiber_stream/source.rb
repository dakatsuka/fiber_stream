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

    # Creates a backpressure-aware source definition from Ractor ports.
    #
    # `port` is the data/control port received by FiberStream. `ack_port` is a
    # producer-owned port that receives `RactorPort::Ack` and
    # `RactorPort::Cancel` control messages. The producer must wait for an ack
    # before sending each `RactorPort::Element`, `RactorPort::Complete`, or
    # `RactorPort::Failure` message.
    def self.ractor_port(port, ack_port:, ack_transfer: :copy, cancel: true)
      raise TypeError, "port must respond to receive" unless port.respond_to?(:receive)
      unless ack_port.respond_to?(:send) && ack_port.method(:send).owner != Kernel
        raise TypeError, "ack_port must provide Ractor-style send"
      end

      Flow.__send__(:validate_ractor_transfer_policy!, :ack_transfer, ack_transfer)
      raise TypeError, "cancel must be true or false" unless [true, false].include?(cancel)

      new(-> { Pull.ractor_port(port, ack_port, ack_transfer, cancel) })
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

    # Returns a new source definition that emits this source, then `source`.
    #
    # Construction is lazy. The appended source is not materialized or pulled
    # until downstream demand observes completion from this source. Flows
    # attached before concat stay scoped to their source; flows attached after
    # concat apply to the combined output.
    def concat(source)
      raise TypeError, "expected FiberStream::Source" unless source.is_a?(Source)

      self.class.__send__(
        :new,
        -> { Pull.concat(materializer, source.__send__(:materializer)) }
      )
    end

    # Returns a new source definition that maps each element with `block`.
    #
    # This is a convenience wrapper around `via(FiberStream::Flow.map { ... })`
    # and has the same lazy construction, error, and backpressure behavior as
    # the underlying flow.
    def map(&block)
      via(Flow.map(&block))
    end

    # Returns a new source definition that maps elements concurrently.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.parallel_map(concurrency:) { ... })` and preserves
    # the same ordered delivery, scheduler requirement, validation, bounded
    # upstream run-ahead, and cancellation behavior.
    def parallel_map(concurrency:, &block)
      via(Flow.parallel_map(concurrency: concurrency, &block))
    end

    # Returns a new source definition that maps elements in Ractor workers.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.ractor_map(workers:) { ... })` and preserves the
    # same shareable mapper requirement, ordered delivery, transfer policy,
    # bounded upstream run-ahead, and cooperative worker shutdown behavior.
    def ractor_map(workers:, input_transfer: :copy, output_transfer: :copy, &block)
      via(
        Flow.ractor_map(
          workers: workers,
          input_transfer: input_transfer,
          output_transfer: output_transfer,
          &block
        )
      )
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

    # Returns a new source definition that drops the first `count` elements.
    #
    # This is a convenience wrapper around `via(FiberStream::Flow.drop(count))`
    # and preserves the same validation and pull-driven backpressure behavior.
    def drop(count)
      via(Flow.drop(count))
    end

    # Returns a new source definition that emits leading elements while `block`
    # is truthy.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.take_while { ... })` and preserves the same
    # predicate truthiness, early completion, and upstream close behavior.
    def take_while(&block)
      via(Flow.take_while(&block))
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

    # Returns a new source definition that splits String chunks into lines.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.lines(chomp:, max_length:))`.
    def lines(chomp: true, max_length: nil)
      via(Flow.lines(chomp: chomp, max_length: max_length))
    end

    # Returns a runnable pipeline from this source to `sink`.
    #
    # Construction is lazy. The source and sink are not materialized until
    # `Pipeline#run` is called.
    def to(sink)
      raise TypeError, "expected FiberStream::Sink" unless sink.is_a?(Sink)

      Pipeline.__send__(:new, self, sink)
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
        stream = materialize

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

    private

    def materializer
      -> { materialize }
    end

    def materialize
      stream = nil

      begin
        stream = @source_factory.call
        @flows.each do |flow|
          stream = flow.__send__(:attach, stream)
        end
        stream
      rescue StandardError
        begin
          stream&.close
        rescue StandardError
          nil
        end
        raise
      end
    end
  end
end
