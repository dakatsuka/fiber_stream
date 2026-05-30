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
        return unless @producer&.alive?
        return if @producer.equal?(Fiber.current)

        @producer.kill
      rescue FiberError
        nil
      end
    end

    private_constant :DONE, :Each, :Map, :Select, :Take, :AsyncBoundary
  end
end
