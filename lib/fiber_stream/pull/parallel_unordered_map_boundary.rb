# frozen_string_literal: true

module FiberStream
  module Pull
    # Unordered scheduler-backed worker boundary for
    # `Flow.parallel_unordered_map`.
    #
    # A single dispatcher pulls upstream and a bounded worker pool maps values.
    # Downstream emits worker results in completion order. Admission is
    # permit-based to keep queued, running, and completed pulled-but-unemitted
    # work bounded by the configured concurrency.
    class ParallelUnorderedMapBoundary
      TERMINAL_RESULT_CAPACITY = 1
      CancellationError = Class.new(StandardError)
      JobMessage = Data.define(:sequence, :value)
      ValueMessage = Data.define(:sequence, :value)
      DoneMessage = Data.define
      ErrorMessage = Data.define(:sequence, :error)
      CloseErrorMessage = Data.define(:error)
      private_constant :JobMessage, :ValueMessage, :DoneMessage, :ErrorMessage, :CloseErrorMessage

      def initialize(upstream, concurrency, transform)
        @upstream = upstream
        @concurrency = concurrency
        @transform = transform
        @permits = Thread::SizedQueue.new(concurrency)
        @jobs = Thread::SizedQueue.new(concurrency)
        @results = Thread::SizedQueue.new(concurrency + TERMINAL_RESULT_CAPACITY)
        @workers = []
        @dispatcher = nil
        @next_sequence = 0
        @outstanding_jobs = 0
        @terminal_message = nil
        @started = false
        @closed = false
        @done = false
        @admission_closed = false
        @upstream_closing = false
        @upstream_closed = false
        @upstream_close_error = nil
        @upstream_close_done = Thread::SizedQueue.new(1)

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
          return emit_terminal(@terminal_message) if terminal_ready?

          message = @results.pop
          return complete if message.nil?

          case message
          in ValueMessage[sequence:, value:]
            return emit_value(sequence, value)
          in DoneMessage | CloseErrorMessage
            @terminal_message = message
          in ErrorMessage[sequence:, error:]
            fail_with_error(sequence, error)
          end
        end
      end

      def emit_value(_sequence, value)
        @outstanding_jobs -= 1
        return_permit unless @admission_closed
        value
      end

      def terminal_ready?
        @terminal_message && @outstanding_jobs.zero?
      end

      def emit_terminal(message)
        case message
        in DoneMessage
          complete
        in CloseErrorMessage[error:]
          fail_with_error(@next_sequence, error, close_admission: false)
        end
      end

      def fail_with_error(_sequence, error, close_admission: true)
        @done = true
        close_admission() if close_admission
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
          if message.is_a?(JobMessage)
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
        @outstanding_jobs += 1
        JobMessage.new(sequence:, value:)
      rescue StandardError => error
        close_upstream(record_error: false)
        ErrorMessage.new(sequence: @next_sequence, error:)
      end

      def terminal_done_message
        close_error = close_upstream
        if close_error
          CloseErrorMessage.new(error: close_error)
        else
          DoneMessage.new
        end
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
        sequence = message.sequence
        value = message.value
        ValueMessage.new(sequence:, value: @transform.call(value))
      rescue CancellationError
        raise
      rescue StandardError => error
        ErrorMessage.new(sequence:, error:)
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
        return wait_for_upstream_close(record_error:) if @upstream_closing
        return nil if @upstream_closed

        @upstream_closing = true
        @upstream.close
        nil
      rescue StandardError => error
        @upstream_close_error ||= error if record_error
        error
      ensure
        if @upstream_closing
          @upstream_closed = true
          @upstream_closing = false
          signal_upstream_close_done
        end
      end

      def wait_for_upstream_close(record_error:)
        @upstream_close_done.pop
        return @upstream_close_error if record_error

        nil
      rescue ClosedQueueError
        record_error ? @upstream_close_error : nil
      end

      def signal_upstream_close_done
        @upstream_close_done << true
      rescue ClosedQueueError
        nil
      ensure
        @upstream_close_done.close
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
            "Flow.parallel_unordered_map requires a non-blocking fiber"
          else
            "Flow.parallel_unordered_map requires Fiber.scheduler"
          end
        raise SchedulerRequiredError, message
      end
    end
  end
end
