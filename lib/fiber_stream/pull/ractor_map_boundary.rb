# frozen_string_literal: true

module FiberStream
  module Pull
    # Ordered Ractor-backed worker boundary for `Flow.ractor_map`.
    #
    # Upstream is pulled by the downstream caller, while blocking waits for
    # Ractor worker messages are isolated in a coordinator thread. The boundary
    # admits work only when a worker is ready and the pulled-but-unemitted count
    # is below `workers`, preserving bounded backpressure and ordered output.
    class RactorMapBoundary
      TERMINAL_RESULT_CAPACITY = 1
      READY_WAIT_INTERVAL = 0.001

      def initialize(upstream, workers, input_transfer, output_transfer, transform)
        @upstream = upstream
        @workers_count = workers
        @input_transfer = input_transfer
        @output_transfer = output_transfer
        @transform = transform
        @result_port = nil
        @ready_workers = Thread::SizedQueue.new(workers)
        @results = Thread::SizedQueue.new(workers + TERMINAL_RESULT_CAPACITY)
        @workers = []
        @active_sequences = {}
        @worker_state_mutex = Mutex.new
        @coordinator = nil
        @pending = {}
        @next_sequence = 0
        @next_emit_sequence = 0
        @in_flight = 0
        @failure_sequence = nil
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
          @workers << self.class.__send__(
            :spawn_worker,
            worker_id,
            @result_port,
            @transform,
            @output_transfer
          )
        end
        @coordinator = Thread.new { run_coordinator }
      end

      def next_message
        loop do
          fill_capacity

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

      def fill_capacity
        return if @admission_closed

        while @in_flight < @workers_count
          worker = take_ready_worker(block: @in_flight.zero?)
          break unless worker

          message = pull_job_message
          if message.fetch(0) == :job
            @in_flight += 1
            break unless deliver_job(worker, message)
          else
            close_admission(close_upstream: false)
            record_result(message)
            break
          end
        end
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

      def deliver_job(worker, message)
        sequence = message.fetch(1)
        track_worker_job(worker, sequence)

        if @input_transfer == :move
          worker.send(message, move: true)
        else
          worker.send(message)
        end
        true
      rescue StandardError => error
        clear_worker_job(worker)
        sequence = message.fetch(1)
        record_result([:error, sequence, build_ractor_map_error(sequence, :input_transfer, error)])
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
        @in_flight -= 1 if @in_flight.positive?
        value
      end

      def fail_with_ordered_error(message)
        sequence = message.fetch(1)
        error = message.fetch(2)

        if @failure_sequence && sequence > @failure_sequence
          @next_emit_sequence = sequence + 1
          @in_flight -= 1 if @in_flight.positive?
          return next_message
        end

        @done = true
        close_admission
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

      def record_result(message)
        if message.fetch(0) == :error
          sequence = message.fetch(1)
          @failure_sequence = sequence if @failure_sequence.nil? || sequence < @failure_sequence
          close_admission
          request_worker_shutdown
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
        case message.fetch(0)
        when :ready
          deliver_ready_worker(message.fetch(1))
          0
        when :value
          handle_worker_value_message(message)
          0
        when :error
          handle_worker_error_message(message)
          0
        when :stopped
          handle_worker_stopped_message(message, live_workers)
        end
      end

      def handle_worker_value_message(message)
        worker = worker_for_id(message.fetch(1))
        sequence = message.fetch(2)
        value = message.fetch(3)

        clear_worker_job(worker)
        deliver_result([:value, sequence, value])
      end

      def handle_worker_error_message(message)
        worker = worker_for_id(message.fetch(1))

        clear_worker_job(worker)
        deliver_result(normalize_worker_error_message(message))
      end

      def handle_worker_stopped_message(message, live_workers)
        worker = worker_for_id(message.fetch(1))
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

        deliver_result([:error, sequence, error])
      end

      def deliver_ready_worker(worker_id)
        return if @closed

        push_until_delivered_or_closed(@ready_workers, worker_for_id(worker_id), suppress_data: false)
      end

      def deliver_result(message)
        return if @closed

        push_until_delivered_or_closed(@results, message, suppress_data: true)
      end

      def push_until_delivered_or_closed(queue, message, suppress_data:)
        loop do
          return if @closed && suppress_data
          return if @closed && !suppress_data

          queue.push(message, true)
          return
        rescue ThreadError, ClosedQueueError
          sleep READY_WAIT_INTERVAL
        end
      end

      def normalize_worker_error_message(message)
        sequence = message.fetch(2)
        kind = message.fetch(3)
        cause_class_name = message.fetch(4)
        cause_message = message.fetch(5)
        error =
          RactorMapError.new(
            sequence: sequence,
            kind: kind,
            cause_class_name: cause_class_name,
            cause_message: cause_message
          )

        [:error, sequence, error]
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
          worker.send([:shutdown])
        rescue StandardError
          nil
        end
      end

      def wait_for_workers
        return unless @coordinator

        sleep READY_WAIT_INTERVAL while @coordinator.alive?
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

      def self.spawn_worker(worker_id, result_port, transform, output_transfer)
        Ractor.new(worker_id, result_port, transform, output_transfer) do |id, port, mapper, transfer|
          current_sequence = nil

          begin
            port.send([:ready, id])

            loop do
              message = Ractor.receive
              break if message.fetch(0) == :shutdown

              current_sequence = message.fetch(1)
              value = message.fetch(2)
              begin
                mapped_value = mapper.call(value)
              rescue Exception => error # rubocop:disable Lint/RescueException
                port.send([:error, id, current_sequence, :worker, error.class.name, error.message])
              else
                begin
                  if transfer == :move
                    port.send([:value, id, current_sequence, mapped_value], move: true)
                  else
                    port.send([:value, id, current_sequence, mapped_value])
                  end
                rescue Exception => error # rubocop:disable Lint/RescueException
                  port.send([:error, id, current_sequence, :output_transfer, error.class.name, error.message])
                end
              end

              current_sequence = nil
              port.send([:ready, id])
            end
          rescue Exception => error # rubocop:disable Lint/RescueException
            sequence = current_sequence || -1
            port.send([:error, id, sequence, :worker_termination, error.class.name, error.message])
          ensure
            port.send([:stopped, id])
          end
        end
      end

      private_class_method :spawn_worker
    end
  end
end
