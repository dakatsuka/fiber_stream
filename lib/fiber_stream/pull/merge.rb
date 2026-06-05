# frozen_string_literal: true

module FiberStream
  module Pull
    # Scheduler-backed ready-order source merge.
    #
    # Each input source is materialized by a scheduled producer fiber on first
    # downstream demand. Producers publish values, completion, and failures into
    # a bounded mailbox; downstream emits values in mailbox arrival order while
    # preserving each input's own order.
    class Merge
      SIDE_ORDER = [:left, :right].freeze
      CancellationError = Class.new(StandardError)
      ValueMessage = Data.define(:side, :value)
      DoneMessage = Data.define(:side)
      ErrorMessage = Data.define(:side, :error)
      private_constant :ValueMessage, :DoneMessage, :ErrorMessage

      def initialize(left_materializer, right_materializer)
        @materializers = { left: left_materializer, right: right_materializer }
        @streams = { left: nil, right: nil }
        @stream_closed = { left: false, right: false }
        @side_done = { left: false, right: false }
        @producers = {}
        @mailbox = nil
        @started = false
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        start
        next_message
      end

      def close
        return if @closed

        @closed = true
        @done = true
        close_error = close_materialized_streams
        close_mailbox
        raise close_error if close_error
      ensure
        cancel_producers
      end

      private

      def start
        return if @started

        validate_scheduler!

        @mailbox = MergeMailbox.new(1)
        @started = true
        SIDE_ORDER.each do |side|
          @producers[side] = Fiber.schedule { run_producer(side) }
        end
      end

      def next_message
        loop do
          message = @mailbox.pop
          return complete if message.nil?

          case message
          in ValueMessage[value:]
            return value
          in DoneMessage[side:]
            mark_side_done(side)
            return complete if all_done?
          in ErrorMessage[error:]
            return fail_with(error)
          end
        end
      rescue MergeMailbox::Closed
        complete
      end

      def run_producer(side)
        stream = materialize_side(side)

        loop do
          break if @closed

          message = pull_message(side, stream)
          break unless deliver(message)
          break unless message.is_a?(ValueMessage)
        end
      rescue MergeMailbox::Closed, CancellationError
        nil
      rescue StandardError => error
        close_side(side, record_error: false)
        deliver(ErrorMessage.new(side:, error:)) unless @closed
      end

      def materialize_side(side)
        stream = @materializers.fetch(side).call
        @streams[side] = stream
        close_side(side) if @closed
        stream
      end

      def pull_message(side, stream)
        value = stream.next
        return terminal_done_message(side) if Pull.done?(value)

        ValueMessage.new(side:, value:)
      rescue StandardError => error
        close_side(side, record_error: false)
        ErrorMessage.new(side:, error:)
      end

      def terminal_done_message(side)
        close_error = close_side(side)
        close_error ? ErrorMessage.new(side:, error: close_error) : DoneMessage.new(side:)
      end

      def deliver(message)
        @mailbox.push(message)
        true
      rescue MergeMailbox::Closed
        false
      end

      def mark_side_done(side)
        @side_done[side] = true
      end

      def all_done?
        SIDE_ORDER.all? { |side| @side_done.fetch(side) }
      end

      def complete
        @done = true
        close_mailbox
        DONE
      end

      def fail_with(error)
        @done = true
        close_mailbox
        close_materialized_streams
        cancel_producers
        raise error
      end

      def close_materialized_streams
        first_error = nil

        SIDE_ORDER.each do |side|
          close_error = close_side(side)
          first_error ||= close_error
        end

        first_error
      end

      def close_side(side, record_error: true)
        return nil if @stream_closed.fetch(side)

        stream = @streams[side]
        return nil unless stream

        @stream_closed[side] = true
        @streams[side] = nil
        stream.close
        nil
      rescue StandardError => error
        error if record_error
      end

      def close_mailbox
        @mailbox&.close
      end

      def cancel_producers
        scheduler = Fiber.scheduler
        return unless scheduler.respond_to?(:fiber_interrupt)

        @producers.each_value do |fiber|
          next unless fiber&.alive?

          scheduler.fiber_interrupt(fiber, CancellationError.new)
        rescue StandardError
          nil
        end
      end

      def validate_scheduler!
        return if Fiber.scheduler && !Fiber.current.blocking?

        message =
          if Fiber.scheduler
            "Source.merge requires a non-blocking fiber"
          else
            "Source.merge requires Fiber.scheduler"
          end
        raise SchedulerRequiredError, message
      end

      class MergeMailbox
        Closed = Class.new(StandardError)

        def initialize(capacity)
          @queue = Thread::SizedQueue.new(capacity)
        end

        def push(message)
          @queue << message
        rescue ClosedQueueError
          raise Closed
        end

        def pop
          @queue.pop
        rescue ClosedQueueError
          raise Closed
        end

        def close
          @queue.close
        end
      end
    end
  end
end
