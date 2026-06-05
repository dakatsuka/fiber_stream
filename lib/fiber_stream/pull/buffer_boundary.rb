# frozen_string_literal: true

module FiberStream
  module Pull
    # Bounded asynchronous prefetch boundary for `Flow.buffer(count)`.
    #
    # The producer task is scheduled lazily and pushes messages into a
    # `Thread::SizedQueue`, so upstream can run ahead only up to the configured
    # queue capacity plus in-flight producer/consumer work. Close is responsible
    # for closing upstream and waking any producer blocked on a full queue.
    class BufferBoundary
      ValueMessage = Data.define(:value)
      DoneMessage = Data.define
      ErrorMessage = Data.define(:error)
      private_constant :ValueMessage, :DoneMessage, :ErrorMessage

      def initialize(upstream, count)
        @upstream = upstream
        @queue = Thread::SizedQueue.new(count)
        @producer = nil
        @started = false
        @closed = false
        @done = false
        @upstream_closed = false
        @upstream_close_error = nil
      end

      def next
        return DONE if @closed || @done

        start
        message = @queue.pop
        return complete if message.nil?

        case message
        in ValueMessage[value:]
          value
        in DoneMessage
          complete
        in ErrorMessage[error:]
          @done = true
          raise error
        end
      end

      def close
        return if @closed

        @closed = true
        @done = true
        close_error = close_upstream
        close_queue
        close_error ||= @upstream_close_error
        raise close_error if close_error
      ensure
        cancel_producer
      end

      private

      def start
        return if @started
        raise SchedulerRequiredError, "Flow.buffer requires Fiber.scheduler" unless Fiber.scheduler

        @started = true
        @producer = Fiber.schedule { run_producer }
      end

      def run_producer
        loop do
          break if @closed

          message = pull_message
          break unless deliver(message)
          break unless message.is_a?(ValueMessage)
        end
      ensure
        @upstream_close_error ||= close_upstream unless @upstream_closed
      end

      def pull_message
        value = @upstream.next
        return terminal_done_message if Pull.done?(value)

        ValueMessage.new(value:)
      rescue StandardError => error
        close_upstream(record_error: false)
        ErrorMessage.new(error:)
      end

      def terminal_done_message
        close_error = close_upstream
        close_error ? ErrorMessage.new(error: close_error) : DoneMessage.new
      end

      def deliver(message)
        @queue << message
        true
      rescue ClosedQueueError
        false
      end

      def close_queue
        @queue.close
      end

      def close_upstream(record_error: true)
        return nil if @upstream_closed

        @upstream_closed = true
        @upstream.close
        nil
      rescue StandardError => error
        @upstream_close_error ||= error if record_error
        error
      end

      def complete
        @done = true
        DONE
      end

      def cancel_producer
        nil
      end
    end
  end
end
