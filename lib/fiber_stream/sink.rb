# frozen_string_literal: true

module FiberStream
  class Sink
    # Creates a sink that collects all stream elements into an Array.
    #
    # The sink consumes upstream until normal completion and returns the
    # collected array as the stream materialized value.
    def self.to_a
      new do |stream|
        values = []

        Pull.each_value(stream) do |value|
          values << value
        end

        values
      end
    end

    # Creates a sink that returns the first stream element.
    #
    # The sink pulls at most one element. It returns `nil` when upstream
    # completes before producing a value.
    def self.first
      new do |stream|
        value = stream.next
        Pull.done?(value) ? nil : value
      end
    end

    # Creates a sink that returns the first element matching a predicate.
    #
    # The sink pulls upstream until the block returns a truthy value or
    # upstream completes. It returns the original matching element, or `nil`
    # when no element matches. Matching `nil` elements are returned as `nil`,
    # following Ruby's `Enumerable#find` ambiguity.
    def self.find(&block)
      raise ArgumentError, "missing block" unless block

      new do |stream|
        loop do
          value = stream.next
          break nil if Pull.done?(value)
          break value if block.call(value)
        end
      end
    end

    # Creates a sink that counts all stream elements.
    #
    # The sink consumes upstream until normal completion and returns the number
    # of elements observed. It does not store consumed elements.
    def self.count
      new do |stream|
        count = 0

        Pull.each_value(stream) do
          count += 1
        end

        count
      end
    end

    # Creates a sink that folds all stream elements into an accumulator.
    #
    # The sink consumes upstream until normal completion. It returns the final
    # accumulator, or the initial accumulator when upstream is empty. Exceptions
    # raised by the block fail the stream and are re-raised from
    # `Source#run_with`. FiberStream assigns the initial accumulator directly;
    # it does not duplicate or freeze that object.
    def self.fold(initial, &block)
      raise ArgumentError, "missing block" unless block

      new do |stream|
        accumulator = initial

        Pull.each_value(stream) do |value|
          accumulator = block.call(accumulator, value)
        end

        accumulator
      end
    end

    # Creates a sink that runs a block for each stream element.
    #
    # The sink consumes upstream until normal completion, calls the block once
    # per element in input order, and returns the number of elements whose block
    # completed successfully. Exceptions raised by the block fail the stream and
    # are re-raised from `Source#run_with`.
    def self.foreach(&block)
      raise ArgumentError, "missing block" unless block

      new do |stream|
        count = 0

        Pull.each_value(stream) do |value|
          block.call(value)
          count += 1
        end

        count
      end
    end

    # Creates a sink that writes String chunks to an IO-like object.
    #
    # The sink consumes upstream until normal completion and returns the number
    # of chunks successfully written. It requires a scheduler-backed
    # non-blocking fiber before write, flush, or normal close operations. The IO
    # object is closed only when `close: true` is passed, and flushed on normal
    # completion only when `flush: true` is passed.
    def self.io(io, close: false, flush: false)
      raise TypeError, "io must respond to write" unless io.respond_to?(:write)
      raise TypeError, "close must be true or false" unless [true, false].include?(close)
      raise TypeError, "flush must be true or false" unless [true, false].include?(flush)
      raise TypeError, "io must respond to close" if close && !io.respond_to?(:close)
      raise TypeError, "io must respond to flush" if flush && !io.respond_to?(:flush)

      new do |stream|
        IOSink.new(io, close, flush).run(stream)
      end
    end

    def self.build(&run) # :nodoc:
      new(&run)
    end

    def initialize(&run)
      @run = run
    end

    private_class_method :new

    def run_stream(stream) # :nodoc:
      @run.call(stream)
    end

    class IOSink
      def initialize(io, close_io, flush_io)
        @io = io
        @close_io = close_io
        @flush_io = flush_io
        @chunks_written = 0
        @io_closed = false
      end

      def run(stream)
        Pull.each_value(stream) do |value|
          write(value)
        end

        finish
      rescue StandardError
        close_suppressing_error
        raise
      end

      private

      def write(value)
        unless value.is_a?(String)
          raise TypeError, "Sink.io elements must be String"
        end

        validate_scheduler!
        @io.write(value)
        @chunks_written += 1
      end

      def finish
        flush
        close_on_normal_completion
        @chunks_written
      end

      def flush
        return unless @flush_io

        validate_scheduler!
        @io.flush
      end

      def close_on_normal_completion
        return unless @close_io

        validate_scheduler!
        close_error = close_io
        raise close_error if close_error
      end

      def validate_scheduler!
        return if Fiber.scheduler && !Fiber.current.blocking?

        message =
          if Fiber.scheduler
            "Sink.io requires a non-blocking fiber"
          else
            "Sink.io requires Fiber.scheduler"
          end
        raise SchedulerRequiredError, message
      end

      def close_suppressing_error
        close_io
      end

      def close_io
        return nil unless @close_io
        return nil if @io_closed

        @io_closed = true
        @io.close
        nil
      rescue StandardError => error
        error
      end
    end

    private_constant :IOSink
  end
end
