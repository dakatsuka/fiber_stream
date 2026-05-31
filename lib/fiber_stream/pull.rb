# frozen_string_literal: true

module FiberStream
  module Pull
    DONE = Object.new.freeze

    def self.done?(value)
      value.equal?(DONE)
    end

    def self.each(enumerable)
      Each.new(enumerable)
    end

    def self.map(upstream, transform)
      Map.new(upstream, transform)
    end

    def self.select(upstream, predicate)
      Select.new(upstream, predicate)
    end

    def self.take(upstream, count)
      Take.new(upstream, count)
    end

    def self.async(upstream)
      AsyncBoundary.new(upstream)
    end

    def self.buffer(upstream, count)
      BufferBoundary.new(upstream, count)
    end

    class Each
      def initialize(enumerable)
        @iterator = enumerable.to_enum(:each)
        @closed = false
      end

      def next
        return DONE if @closed

        @iterator.next
      rescue StopIteration
        DONE
      end

      def close
        return if @closed

        @closed = true
      end
    end

    class Map
      def initialize(upstream, transform)
        @upstream = upstream
        @transform = transform
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        value = @upstream.next
        if Pull.done?(value)
          @done = true
          return DONE
        end

        @transform.call(value)
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end

    class Select
      def initialize(upstream, predicate)
        @upstream = upstream
        @predicate = predicate
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        loop do
          value = @upstream.next
          if Pull.done?(value)
            @done = true
            return DONE
          end

          return value if @predicate.call(value)
        end
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end

    class Take
      def initialize(upstream, count)
        @upstream = upstream
        @remaining = count
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        if @remaining.zero?
          @done = true
          close
          return DONE
        end

        value = @upstream.next
        if Pull.done?(value)
          @done = true
          return DONE
        end

        @remaining -= 1
        close if @remaining.zero?

        value
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end

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

    class BufferBoundary
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
          break unless message.fetch(0) == :value
        end
      ensure
        @upstream_close_error ||= close_upstream unless @upstream_closed
      end

      def pull_message
        value = @upstream.next
        return terminal_done_message if Pull.done?(value)

        [:value, value]
      rescue StandardError => error
        close_upstream(record_error: false)
        [:error, error]
      end

      def terminal_done_message
        close_error = close_upstream
        close_error ? [:error, close_error] : [:done]
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

    private_constant :DONE, :Each, :Map, :Select, :Take, :AsyncBoundary,
                     :BufferBoundary
  end
end
