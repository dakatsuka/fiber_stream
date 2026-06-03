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
      def initialize(upstream)
        @upstream = upstream
        @producer = nil
        @started = false
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        start
        message = @producer.resume

        case message.fetch(0)
        when :value
          message.fetch(1)
        when :done
          complete
        when :error
          @done = true
          raise message.fetch(1)
        end
      end

      def close
        return if @closed

        @closed = true
        @done = true
        @upstream.close
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
            Fiber.yield([:done])
            break
          end

          Fiber.yield([:value, value])
        end
      rescue StandardError => exception
        Fiber.yield([:error, exception]) unless @closed
      ensure
        @upstream.close
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
