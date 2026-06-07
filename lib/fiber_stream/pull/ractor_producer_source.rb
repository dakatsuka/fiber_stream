# frozen_string_literal: true

module FiberStream
  module Pull
    # Setup adapter for high-level owned Ractor producer sources.
    #
    # Producer ractors and ports are created on first demand. Once every
    # producer has returned its producer-owned ack port, this adapter delegates
    # demand to the existing low-level Ractor port pull sources.
    class RactorProducerSource
      StartedProducer = Data.define(:side, :data_port, :setup_port, :ractor, :definition)
      ReadyProducer = Data.define(:side, :data_port, :ack_port, :ractor)
      PortPair = Data.define(:port, :ack_port, :producer_ractor)
      SetupSuccess = Data.define(:producers)
      SetupError = Data.define(:error)
      SetupClosed = Data.define
      private_constant :StartedProducer, :ReadyProducer, :PortPair, :SetupSuccess, :SetupError, :SetupClosed

      def initialize(definitions, ack_transfer, merge)
        @definitions = definitions
        @ack_transfer = ack_transfer
        @merge = merge
        @setup_results = Thread::SizedQueue.new(1)
        @state_mutex = Mutex.new
        @started = false
        @closed = false
        @done = false
        @shutdown_port = nil
        @setup_thread = nil
        @delegate = nil
        @started_producers = []
        @ready_producers = []
      end

      def next
        return DONE if closed_or_done?

        start
        return DONE unless ensure_delegate

        value = @delegate.next
        mark_done if Pull.done?(value)
        value
      end

      def close
        already_closed = mark_closed
        return if already_closed

        wake_setup
        wait_for_setup
        close_setup_queue
        close_error = close_delegate
        cancel_ready_producers
        wait_for_ractors(@ready_producers.map(&:ractor))
        raise close_error if close_error
      end

      private

      def closed_or_done?
        @state_mutex.synchronize { @closed || @done }
      end

      def mark_done
        @state_mutex.synchronize { @done = true }
      end

      def mark_closed
        @state_mutex.synchronize do
          already_closed = @closed
          @closed = true
          @done = true
          already_closed
        end
      end

      def start
        @state_mutex.synchronize do
          return if @started

          @started = true
          @shutdown_port = Ractor::Port.new
          spawn_producers
          @setup_thread = Thread.new { run_setup }
        end
      rescue Exception => error # rubocop:disable Lint/RescueException
        setup_error = build_error(:producer_setup, error)
        @setup_thread = Thread.new { cleanup_after_start_failure(setup_error) }
      end

      def cleanup_after_start_failure(setup_error)
        setup_ports = @started_producers.map(&:setup_port)
        ractors = @started_producers.map(&:ractor)
        producer_by_setup_port = @started_producers.to_h { |producer| [producer.setup_port, producer] }

        cleanup_remaining_setup(setup_ports, ractors, producer_by_setup_port)
        deliver_setup(SetupError.new(error: setup_error))
      end

      def spawn_producers
        @started_producers = []

        @definitions.each_with_index do |definition, side|
          data_port = Ractor::Port.new
          setup_port = Ractor::Port.new
          ractor = self.class.__send__(:spawn_producer, data_port, setup_port, definition)
          @started_producers << StartedProducer.new(side:, data_port:, setup_port:, ractor:, definition:)
        end
      end

      def run_setup
        remaining_setup_ports = @started_producers.map(&:setup_port)
        remaining_ractors = @started_producers.map(&:ractor)
        producer_by_setup_port = @started_producers.to_h { |producer| [producer.setup_port, producer] }

        until remaining_setup_ports.empty?
          selected, message = Ractor.select(@shutdown_port, *remaining_setup_ports, *remaining_ractors)
          if selected == @shutdown_port
            deliver_setup(SetupClosed.new)
            cleanup_remaining_setup(remaining_setup_ports, remaining_ractors, producer_by_setup_port)
            return
          elsif producer_by_setup_port.key?(selected)
            producer = producer_by_setup_port.fetch(selected)
            validate_ack_port!(message)
            @ready_producers << ReadyProducer.new(
              side: producer.side,
              data_port: producer.data_port,
              ack_port: message,
              ractor: producer.ractor
            )
            remaining_setup_ports.delete(selected)
          else
            raise "producer exited before setup completed"
          end
        end

        deliver_setup(SetupSuccess.new(producers: @ready_producers.sort_by(&:side).freeze))
      rescue Exception => error # rubocop:disable Lint/RescueException
        setup_error = build_error(:producer_setup, error)
        cancel_ready_producers
        cleanup_remaining_setup(remaining_setup_ports, remaining_ractors, producer_by_setup_port)
        deliver_setup(SetupError.new(error: setup_error))
      end

      def cleanup_remaining_setup(remaining_setup_ports, remaining_ractors, producer_by_setup_port)
        producer_by_ractor = @started_producers.to_h { |producer| [producer.ractor, producer] }

        until remaining_setup_ports.empty?
          selected, message = Ractor.select(*remaining_setup_ports, *remaining_ractors)
          if producer_by_setup_port.key?(selected)
            producer = producer_by_setup_port.fetch(selected)
            validate_ack_port!(message)
            ready = ReadyProducer.new(
              side: producer.side,
              data_port: producer.data_port,
              ack_port: message,
              ractor: producer.ractor
            )
            @ready_producers << ready
            send_cancel(ready)
            remaining_setup_ports.delete(selected)
          else
            producer = producer_by_ractor.fetch(selected)
            remaining_setup_ports.delete(producer.setup_port)
            remaining_ractors.delete(selected)
          end
        end

        wait_for_ractors(@started_producers.map(&:ractor))
      rescue Exception # rubocop:disable Lint/RescueException
        nil
      end

      def validate_ack_port!(ack_port)
        return if ack_port.respond_to?(:send) && ack_port.method(:send).owner != Kernel

        raise TypeError, "producer setup did not return a Ractor-style ack port"
      end

      def ensure_delegate
        return true if delegate_installed?

        case setup_result
        in SetupSuccess[producers:]
          delegate = build_delegate(producers)
          should_close_delegate = false
          @state_mutex.synchronize do
            @delegate = delegate
            should_close_delegate = @closed
            @delegate = nil if should_close_delegate
          end
          if should_close_delegate
            close_delegate_suppressing(delegate)
            return false
          end

          true
        in SetupError[error:]
          mark_done
          raise_error(error)
        in SetupClosed
          mark_done
          false
        end
      end

      def setup_result
        result = @setup_results.pop
        result || SetupClosed.new
      rescue ClosedQueueError
        SetupClosed.new
      end

      def build_delegate(producers)
        if @merge
          Pull.ractor_merge_ports(
            producers.map do |producer|
              PortPair.new(port: producer.data_port, ack_port: producer.ack_port, producer_ractor: producer.ractor)
            end,
            @ack_transfer,
            true
          )
        else
          producer = producers.fetch(0)
          Pull.ractor_port(producer.data_port, producer.ack_port, @ack_transfer, true, producer.ractor)
        end
      end

      def cancel_ready_producers
        @ready_producers.each do |producer|
          send_cancel(producer)
        rescue Exception # rubocop:disable Lint/RescueException
          nil
        end
      end

      def send_cancel(producer)
        send_control(producer.ack_port, RactorPort::Cancel.new(:closed))
      end

      def send_control(port, message)
        if @ack_transfer == :move
          port.send(message, move: true)
        else
          port.send(message)
        end
      end

      def close_delegate
        delegate = @state_mutex.synchronize { @delegate }
        delegate&.close
        nil
      rescue StandardError => error
        error
      end

      def close_delegate_suppressing(delegate)
        delegate.close
      rescue StandardError
        nil
      end

      def delegate_installed?
        @state_mutex.synchronize { !@delegate.nil? }
      end

      def wake_setup
        @shutdown_port&.send(:shutdown)
      rescue Exception # rubocop:disable Lint/RescueException
        nil
      end

      def wait_for_setup
        @setup_thread&.join
      end

      def wait_for_ractors(ractors)
        return if ractors.empty?

        Thread.new do
          ractors.each do |ractor|
            ractor.value
          rescue Exception # rubocop:disable Lint/RescueException
            nil
          end
        end.join
      end

      def close_setup_queue
        @setup_results.close
      end

      def deliver_setup(result)
        @setup_results.push(result)
      rescue ClosedQueueError, ThreadError
        nil
      end

      def build_error(kind, error)
        RactorPortSourceError.new(
          kind: kind,
          cause_class_name: error.class.name,
          cause_message: error.message,
          cause: error
        )
      end

      def raise_error(error)
        if error.is_a?(RactorPortSourceError) && error.original_cause
          raise error, cause: error.original_cause
        end

        raise error
      end

      def self.spawn_producer(data_port, setup_port, definition)
        Ractor.new(
          data_port,
          setup_port,
          definition.block,
          definition.transfer,
          definition.args
        ) do |outbox, setup, block, transfer, args|
          ack_port = Ractor::Port.new
          setup.send(ack_port)
          producer = RactorProducer.new(outbox, ack_port, transfer)

          begin
            block.call(producer, *args)
            producer.complete unless producer.__send__(:terminal?) || producer.cancelled?
          rescue Exception => error # rubocop:disable Lint/RescueException
            producer.fail(error) unless producer.__send__(:terminal?) || producer.cancelled?
          end

          if producer.__send__(:send_failed?)
            ProducerSendFailed.new
          elsif producer.cancelled?
            ProducerCancelled.new
          else
            ProducerTerminal.new
          end
        end
      end

      private_class_method :spawn_producer
    end
  end
end
