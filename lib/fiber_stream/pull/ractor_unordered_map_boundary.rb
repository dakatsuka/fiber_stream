# frozen_string_literal: true

module FiberStream
  module Pull
    # Unordered Ractor-backed worker boundary for
    # `Flow.ractor_unordered_map`.
    #
    # Upstream is pulled by the downstream caller, while blocking waits for
    # Ractor worker messages are isolated in a coordinator thread. Downstream
    # emits worker results in completion order and admission stays bounded by
    # the number of workers.
    class RactorUnorderedMapBoundary
      Job = ::Data.define(:sequence, :value)
      Shutdown = ::Data.define
      Ready = ::Data.define(:worker_id)
      WorkerValue = ::Data.define(:worker_id, :sequence, :value)
      WorkerFailure = ::Data.define(:worker_id, :sequence, :kind, :cause_class_name, :cause_message)
      Stopped = ::Data.define(:worker_id)
      ResultValue = ::Data.define(:sequence, :value)
      ResultDone = ::Data.define
      ResultCloseError = ::Data.define(:sequence, :error)
      ResultError = ::Data.define(:sequence, :error)

      private_constant :Job, :Shutdown, :Ready, :WorkerValue, :WorkerFailure, :Stopped
      private_constant :ResultValue, :ResultDone, :ResultCloseError, :ResultError

      def initialize(upstream, workers, input_transfer, output_transfer, transform)
        @upstream = upstream
        @workers_count = workers
        @input_transfer = input_transfer
        @output_transfer = output_transfer
        @transform = transform
        @result_port = nil
        @ready_workers = Thread::SizedQueue.new(workers)
        @results = Thread::SizedQueue.new(workers)
        @workers = []
        @active_sequences = {}
        @worker_state_mutex = Mutex.new
        @coordinator = nil
        @next_sequence = 0
        @outstanding_jobs = 0
        @terminal_message = nil
        @started = false
        @closed = false
        @done = false
        @admission_closed = false
        @worker_shutdown_sent = false
        @upstream_closed = false
        @upstream_close_error = nil
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
        close_admission(close_upstream: false)
        close_ready_queue
        close_result_queue
        request_worker_shutdown
        wait_for_workers
        close_error ||= @upstream_close_error
        raise close_error if close_error
      end

      private

      def start
        return if @started

        @started = true
        @result_port = Ractor::Port.new
        @workers_count.times do |worker_id|
          @workers << self.class.spawn_worker(
            worker_id,
            @result_port,
            @transform,
            @output_transfer
          )
        end
        @coordinator = Thread.new { run_coordinator }
      end

      def next_message
        return emit_terminal(@terminal_message) if terminal_ready?

        ready = take_result(block: false)
        return emit(ready) if ready

        fill_capacity
        return emit_terminal(@terminal_message) if terminal_ready?

        message = take_result(block: true)
        return complete unless message

        emit(message)
      end

      def fill_capacity
        return if @admission_closed

        while @outstanding_jobs < @workers_count
          worker = take_ready_worker(block: @outstanding_jobs.zero? && @terminal_message.nil?)
          break unless worker

          message = pull_job_message
          if message.is_a?(Job)
            @outstanding_jobs += 1
            break unless deliver_job(worker, message)
          elsif upstream_failure?(message)
            fail_with_error(message.sequence, message.error)
          else
            close_admission(close_upstream: false)
            @terminal_message = message
            break
          end
        end
      end

      def pull_job_message
        value = @upstream.next
        return terminal_done_message if Pull.done?(value)

        sequence = @next_sequence
        @next_sequence += 1
        Job.new(sequence, value)
      rescue StandardError => error
        close_upstream(record_error: false)
        ResultError.new(sequence: @next_sequence, error:)
      end

      def terminal_done_message
        close_error = close_upstream
        if close_error
          ResultCloseError.new(sequence: @next_sequence, error: close_error)
        else
          ResultDone.new
        end
      end

      def deliver_job(worker, message)
        sequence = message.sequence
        track_worker_job(worker, sequence)

        if @input_transfer == :move
          worker.send(message, move: true)
        else
          worker.send(message)
        end
        true
      rescue StandardError => error
        clear_worker_job(worker)
        deliver_result(ResultError.new(sequence:, error: build_ractor_map_error(sequence, :input_transfer, error)))
        close_admission
        request_worker_shutdown
        false
      end

      def take_ready_worker(block:)
        if block
          loop do
            worker = @ready_workers.pop
            return worker if worker || @closed || @admission_closed || @ready_workers.closed?
          end
        else
          @ready_workers.pop(true)
        end
      rescue ThreadError, ClosedQueueError
        nil
      end

      def take_result(block:)
        block ? @results.pop : @results.pop(true)
      rescue ThreadError, ClosedQueueError
        nil
      end

      def emit(message)
        case message
        in ResultValue[_sequence, value]
          emit_value(value)
        in ResultError[sequence:, error:]
          fail_with_error(sequence, error)
        end
      end

      def emit_value(value)
        @outstanding_jobs -= 1 if @outstanding_jobs.positive?
        value
      end

      def terminal_ready?
        @terminal_message && @outstanding_jobs.zero?
      end

      def emit_terminal(message)
        case message
        in ResultDone
          complete
        in ResultCloseError[sequence:, error:]
          fail_with_error(sequence, error, close_admission: false)
        in ResultError[sequence:, error:]
          fail_with_error(sequence, error, close_admission: false)
        end
      end

      def upstream_failure?(message)
        message.is_a?(ResultError)
      end

      def fail_with_error(_sequence, error, close_admission: true)
        @done = true
        close_admission() if close_admission
        close_result_queue
        request_worker_shutdown
        if error.is_a?(RactorMapError) && error.original_cause
          raise error, cause: error.original_cause
        end

        raise error
      end

      def complete
        @done = true
        request_worker_shutdown
        DONE
      end

      def run_coordinator
        stopped = 0
        live_workers = @workers.dup

        until stopped == @workers_count
          selected, message = select_worker_message(live_workers)
          if selected == :worker_remote_error
            stopped += 1
          elsif selected == @result_port
            stopped += handle_worker_message(message, live_workers)
          else
            live_workers.delete(selected)
            handle_worker_termination(selected)
            stopped += 1
          end
        end
      ensure
        close_ready_queue
        close_result_queue if @closed
      end

      def select_worker_message(live_workers)
        Ractor.select(@result_port, *live_workers)
      rescue Ractor::RemoteError => error
        worker = remote_error_worker(error, live_workers) || failed_worker_for_remote_error(live_workers)
        live_workers.delete(worker) if worker
        handle_worker_remote_error(worker, error)
        [:worker_remote_error, nil]
      end

      def remote_error_worker(error, live_workers)
        return unless error.respond_to?(:ractor)

        worker = error.ractor
        live_workers.include?(worker) ? worker : nil
      end

      def failed_worker_for_remote_error(live_workers)
        @worker_state_mutex.synchronize do
          live_workers
            .select { |worker| @active_sequences.key?(worker) }
            .min_by { |worker| @active_sequences.fetch(worker) }
        end || live_workers.first
      end

      def handle_worker_remote_error(worker, error)
        sequence = worker ? clear_worker_job(worker) : nil
        sequence ||= @next_sequence
        return if @closed || @worker_shutdown_sent

        deliver_worker_termination_error(worker, sequence, cause: error)
      end

      def handle_worker_message(message, live_workers)
        case message
        in Ready[worker_id]
          deliver_ready_worker(worker_id)
          0
        in WorkerValue
          handle_worker_value_message(message)
          0
        in WorkerFailure
          handle_worker_error_message(message)
          0
        in Stopped
          handle_worker_stopped_message(message, live_workers)
        end
      end

      def handle_worker_value_message(message)
        worker = worker_for_id(message.worker_id)

        clear_worker_job(worker)
        deliver_result(ResultValue.new(sequence: message.sequence, value: message.value))
      end

      def handle_worker_error_message(message)
        worker = worker_for_id(message.worker_id)

        clear_worker_job(worker)
        deliver_result(normalize_worker_error_message(message))
      end

      def handle_worker_stopped_message(message, live_workers)
        worker = worker_for_id(message.worker_id)
        live_workers.delete(worker)
        sequence = clear_worker_job(worker)
        deliver_worker_termination_error(worker, sequence) if sequence && !@closed && !@worker_shutdown_sent
        1
      end

      def handle_worker_termination(worker)
        sequence = clear_worker_job(worker) || @next_sequence
        return if @closed || @worker_shutdown_sent

        deliver_worker_termination_error(worker, sequence)
      end

      def deliver_worker_termination_error(worker, sequence, cause: nil)
        close_ready_queue
        error =
          RactorMapError.new(
            sequence: sequence,
            kind: :worker_termination,
            cause_class_name: cause&.class&.name || worker.class.name,
            cause_message: cause&.message || "worker terminated without a lifecycle message",
            cause: cause
          )

        deliver_result(ResultError.new(sequence:, error:))
      end

      def deliver_ready_worker(worker_id)
        return if @closed

        push_until_delivered_or_closed(@ready_workers, worker_for_id(worker_id))
      end

      def deliver_result(message)
        return if @closed

        push_until_delivered_or_closed(@results, message)
      end

      def push_until_delivered_or_closed(queue, message)
        return if @closed

        queue.push(message)
      rescue ThreadError, ClosedQueueError
        nil
      end

      def normalize_worker_error_message(message)
        sequence = message.sequence
        error =
          RactorMapError.new(
            sequence: sequence,
            kind: message.kind,
            cause_class_name: message.cause_class_name,
            cause_message: message.cause_message
          )

        ResultError.new(sequence:, error:)
      end

      def worker_for_id(worker_id)
        @workers.fetch(worker_id)
      end

      def track_worker_job(worker, sequence)
        @worker_state_mutex.synchronize do
          @active_sequences[worker] = sequence
        end
      end

      def clear_worker_job(worker)
        @worker_state_mutex.synchronize do
          @active_sequences.delete(worker)
        end
      end

      def close_admission(close_upstream: true)
        return if @admission_closed

        @admission_closed = true
        close_upstream(record_error: false) if close_upstream
      end

      def request_worker_shutdown
        return unless @started
        return if @worker_shutdown_sent

        @worker_shutdown_sent = true
        @workers.each do |worker|
          worker.send(Shutdown.new)
        rescue StandardError
          nil
        end
      end

      def wait_for_workers
        return unless @coordinator

        @coordinator.join
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

      def close_ready_queue
        @ready_workers.close
      end

      def close_result_queue
        @results.close
      end

      def build_ractor_map_error(sequence, kind, error)
        RactorMapError.new(
          sequence: sequence,
          kind: kind,
          cause_class_name: error.class.name,
          cause_message: error.message,
          cause: error
        )
      end

      class << self
        def spawn_worker(worker_id, result_port, transform, output_transfer) # :nodoc:
          Ractor.new(worker_id, result_port, transform, output_transfer) do |id, port, mapper, transfer|
            current_sequence = nil
            send_control =
              lambda do |message|
                port.send(message)
                true
              rescue Exception # rubocop:disable Lint/RescueException
                false
              end
            send_failure =
              lambda do |sequence, kind, error|
                send_control.call(WorkerFailure.new(id, sequence, kind, error.class.name, error.message))
              rescue Exception # rubocop:disable Lint/RescueException
                false
              end

            begin
              if send_control.call(Ready.new(id))
                loop do
                  message = Ractor.receive
                  case message
                  in Shutdown
                    break
                  in Job[sequence, value]
                    current_sequence = sequence
                  else
                    raise TypeError, "invalid ractor_unordered_map worker message: #{message.class}"
                  end

                  begin
                    mapped_value = mapper.call(value)
                  rescue Exception => error # rubocop:disable Lint/RescueException
                    break unless send_failure.call(current_sequence, :worker, error)
                  else
                    begin
                      if transfer == :move
                        port.send(WorkerValue.new(id, current_sequence, mapped_value), move: true)
                      else
                        port.send(WorkerValue.new(id, current_sequence, mapped_value))
                      end
                    rescue Exception => error # rubocop:disable Lint/RescueException
                      break unless send_failure.call(current_sequence, :output_transfer, error)
                    end
                  end

                  current_sequence = nil
                  break unless send_control.call(Ready.new(id))
                end
              end
            rescue Exception => error # rubocop:disable Lint/RescueException
              sequence = current_sequence || -1
              send_failure.call(sequence, :worker_termination, error)
            ensure
              send_control.call(Stopped.new(id))
            end
          end
        end
      end
    end
  end
end
