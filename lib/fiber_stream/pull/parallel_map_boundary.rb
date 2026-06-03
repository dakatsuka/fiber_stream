# frozen_string_literal: true

module FiberStream
  module Pull
    # Ordered scheduler-backed worker boundary for `Flow.parallel_map`.
    #
    # A single dispatcher pulls upstream and assigns sequence numbers while a
    # bounded worker pool maps values. Downstream emits results in input order,
    # so admission is permit-based to keep queued, running, and completed
    # pulled-but-unemitted work bounded by the configured concurrency.
    class ParallelMapBoundary
      TERMINAL_RESULT_CAPACITY = 1
      CancellationError = Class.new(StandardError)

      def initialize(upstream, concurrency, transform)
        @upstream = upstream
        @concurrency = concurrency
        @transform = transform
        @permits = Thread::SizedQueue.new(concurrency)
        @jobs = Thread::SizedQueue.new(concurrency)
        @results = Thread::SizedQueue.new(concurrency + TERMINAL_RESULT_CAPACITY)
        @workers = []
        @dispatcher = nil
        @pending = {}
        @next_sequence = 0
        @next_emit_sequence = 0
        @failure_sequence = nil
        @started = false
        @closed = false
        @done = false
        @admission_closed = false
        @upstream_closed = false
        @upstream_close_error = nil

        concurrency.times { @permits << true }
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
        close_error = close_upstream
        close_internal_queues
        close_error ||= @upstream_close_error
        raise close_error if close_error
      ensure
        cancel_fibers
      end

      private

      def start
        return if @started

        validate_scheduler!

        @started = true
        @concurrency.times do
          @workers << Fiber.schedule { run_worker }
        end
        @dispatcher = Fiber.schedule { run_dispatcher }
      end

      def next_message
        loop do
          ready = @pending.delete(@next_emit_sequence)
          if ready
            drain_available_results
            return emit(ready)
          end

          message = @results.pop
          return complete if message.nil?

          record_result(message)
        end
      end

      def emit(message)
        case message.fetch(0)
        when :value
          emit_value(message)
        when :done
          complete
        when :error
          fail_with_ordered_error(message)
        end
      end

      def emit_value(message)
        sequence = message.fetch(1)
        value = message.fetch(2)
        @next_emit_sequence = sequence + 1
        return_permit unless @admission_closed
        value
      end

      def fail_with_ordered_error(message)
        sequence = message.fetch(1)
        error = message.fetch(2)

        if @failure_sequence && sequence > @failure_sequence
          @next_emit_sequence = sequence + 1
          return next_message
        end

        @done = true
        close_result_queue
        cancel_fibers
        raise error
      end

      def complete
        @done = true
        close_result_queue
        DONE
      end

      def run_dispatcher
        loop do
          break if @closed || @admission_closed
          break unless take_permit

          message = pull_job_message
          if message.fetch(0) == :job
            break unless deliver_job(message)
          else
            close_admission(close_upstream: false)
            deliver_result(message)
            break
          end
        end
      rescue CancellationError
        nil
      ensure
        close_upstream unless @upstream_closed || @closed
        close_job_queue
      end

      def pull_job_message
        value = @upstream.next
        return terminal_done_message if Pull.done?(value)

        sequence = @next_sequence
        @next_sequence += 1
        [:job, sequence, value]
      rescue StandardError => error
        close_upstream(record_error: false)
        [:error, @next_sequence, error]
      end

      def terminal_done_message
        close_error = close_upstream
        close_error ? [:error, @next_sequence, close_error] : [:done, @next_sequence]
      end

      def run_worker
        loop do
          break if @closed

          message = @jobs.pop
          break if message.nil?

          deliver_result(map_job(message))
        end
      rescue CancellationError
        nil
      end

      def map_job(message)
        sequence = message.fetch(1)
        value = message.fetch(2)
        [:value, sequence, @transform.call(value)]
      rescue CancellationError
        raise
      rescue StandardError => error
        [:error, sequence, error]
      end

      def record_result(message)
        if message.fetch(0) == :error
          sequence = message.fetch(1)
          @failure_sequence = sequence if @failure_sequence.nil? || sequence < @failure_sequence
          close_admission
        end

        @pending[message.fetch(1)] = message
      end

      def drain_available_results
        loop do
          message = @results.pop(true)
          break if message.nil?

          record_result(message)
        rescue ThreadError
          break
        end
      end

      def close_admission(close_upstream: true)
        return if @admission_closed

        @admission_closed = true
        close_upstream(record_error: false) if close_upstream
        close_permit_queue
        close_job_queue
      end

      def take_permit
        @permits.pop
      rescue ClosedQueueError
        nil
      end

      def return_permit
        @permits << true
      rescue ClosedQueueError
        nil
      end

      def deliver_job(message)
        @jobs << message
        true
      rescue ClosedQueueError
        false
      end

      def deliver_result(message)
        @results << message
        true
      rescue ClosedQueueError
        false
      end

      def close_internal_queues
        close_permit_queue
        close_job_queue
        close_result_queue
      end

      def close_permit_queue
        @permits.close
      end

      def close_job_queue
        @jobs.close
      end

      def close_result_queue
        @results.close
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

      def cancel_fibers
        scheduler = Fiber.scheduler
        return unless scheduler.respond_to?(:fiber_interrupt)

        (@workers + [@dispatcher]).compact.each do |fiber|
          next unless fiber.alive?

          scheduler.fiber_interrupt(fiber, CancellationError.new)
        rescue StandardError
          nil
        end
      end

      def validate_scheduler!
        return if Fiber.scheduler && !Fiber.current.blocking?

        message =
          if Fiber.scheduler
            "Flow.parallel_map requires a non-blocking fiber"
          else
            "Flow.parallel_map requires Fiber.scheduler"
          end
        raise SchedulerRequiredError, message
      end
    end
  end
end
