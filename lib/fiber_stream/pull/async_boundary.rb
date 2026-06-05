# frozen_string_literal: true

module FiberStream
  module Pull
    # One-element asynchronous boundary for `Flow.async`.
    #
    # The producer fiber is created lazily on first downstream demand. It
    # advances upstream in a non-blocking fiber and yields one message at a
    # time back to the downstream caller, so it adds an async boundary without
    # adding prefetch.
    class AsyncBoundary
      ValueMessage = Data.define(:value)
      DoneMessage = Data.define
      ErrorMessage = Data.define(:error)
      private_constant :ValueMessage, :DoneMessage, :ErrorMessage

      def initialize(upstream)
        @upstream = upstream
        @producer = nil
        @started = false
        @closed = false
        @done = false
        @upstream_closed = false
      end

      def next
        return DONE if @closed || @done

        start
        message = @producer.resume

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
        close_upstream
      ensure
        cancel_producer
      end

      private

      def start
        return if @started
        raise SchedulerRequiredError, "Flow.async requires Fiber.scheduler" unless Fiber.scheduler

        @started = true
        @producer = Fiber.new(blocking: false) { run_producer }
      end

      def run_producer
        loop do
          break if @closed

          value = @upstream.next
          if Pull.done?(value)
            Fiber.yield(DoneMessage.new)
            break
          end

          Fiber.yield(ValueMessage.new(value:))
        end
      rescue StandardError => exception
        Fiber.yield(ErrorMessage.new(error: exception)) unless @closed
      ensure
        close_upstream
      end

      def complete
        @done = true
        DONE
      end

      def close_upstream
        return if @upstream_closed

        @upstream_closed = true
        @upstream.close
      end

      def cancel_producer
        return unless @producer&.alive?

        @producer.kill
      rescue StandardError
        nil
      end
    end
  end
end
