# frozen_string_literal: true

module FiberStream
  module Pull
    # Pull stream for `Source.ractor_port`.
    #
    # Downstream demand is converted into one `RactorPort::Ack` sent to the
    # producer-owned acknowledgment port. Blocking Ractor waits are isolated in
    # a coordinator thread so scheduler-managed fibers do not call Ractor wait
    # APIs directly.
    class RactorPortSource
      ProtocolMessage = Data.define(:message)
      ErrorMessage = Data.define(:error)
      ClosedMessage = Data.define
      private_constant :ProtocolMessage, :ErrorMessage, :ClosedMessage

      def initialize(port, ack_port, ack_transfer, cancel)
        @port = port
        @ack_port = ack_port
        @ack_transfer = ack_transfer
        @cancel_enabled = cancel
        @demands = Thread::SizedQueue.new(1)
        @results = Thread::SizedQueue.new(1)
        @shutdown_port = nil
        @coordinator = nil
        @state_mutex = Mutex.new
        @started = false
        @closed = false
        @done = false
        @producer_terminal = false
        @cancel_sent = false
      end

      def next
        return DONE if closed_or_done?

        start
        request_next_message
        result = @results.pop
        return DONE if result.nil?

        handle_result(result)
      end

      def close
        cancel_error = nil

        should_cancel =
          @state_mutex.synchronize do
            return if @closed

            @closed = true
            @done = true
            @cancel_enabled && !@producer_terminal && !@cancel_sent
          end

        cancel_error = send_cancel if should_cancel
        wake_coordinator
        close_result_queue
        wait_for_coordinator
        raise cancel_error if cancel_error
      end

      private

      def closed_or_done?
        @state_mutex.synchronize { @closed || @done }
      end

      def start
        @state_mutex.synchronize do
          return if @started

          @started = true
          @shutdown_port = Ractor::Port.new
          @coordinator = Thread.new { run_coordinator }
        end
      end

      def request_next_message
        @demands.push(:next)
      rescue ClosedQueueError
        nil
      end

      def handle_result(result)
        case result
        in ProtocolMessage[message:]
          handle_protocol_message(message)
        in ErrorMessage[error:]
          mark_done
          raise_error(error)
        in ClosedMessage
          DONE
        end
      end

      def handle_protocol_message(message)
        case message
        in RactorPort::Element[value]
          value
        in RactorPort::Complete
          mark_producer_terminal
          DONE
        in RactorPort::Failure[String => cause_class_name, String => cause_message]
          mark_producer_terminal
          raise RactorPortSourceError.new(
            kind: :producer_failure,
            cause_class_name: cause_class_name,
            cause_message: cause_message
          )
        in RactorPort::Failure
          raise_invalid_message(message, "Failure payloads must be Strings")
        else
          raise_invalid_message(message, "invalid RactorPort message")
        end
      end

      def raise_invalid_message(message, cause_message)
        mark_done
        raise RactorPortSourceError.new(
          kind: :invalid_message,
          cause_class_name: message.class.name,
          cause_message: cause_message
        )
      end

      def mark_done
        @state_mutex.synchronize { @done = true }
      end

      def mark_producer_terminal
        @state_mutex.synchronize do
          @done = true
          @producer_terminal = true
        end
      end

      def raise_error(error)
        if error.is_a?(RactorPortSourceError) && error.original_cause
          raise error, cause: error.original_cause
        end

        raise error
      end

      def run_coordinator
        loop do
          demand = @demands.pop
          break unless demand
          break if closed?

          ack_error = send_ack
          if ack_error
            deliver_result(ErrorMessage.new(error: ack_error))
            break
          end

          selected, message = select_message
          break if selected == @shutdown_port || closed?

          deliver_result(ProtocolMessage.new(message:))
        end
      rescue StandardError => error
        deliver_result(ErrorMessage.new(error: build_error(:receive, error)))
      ensure
        deliver_result(ClosedMessage.new) if closed?
      end

      def select_message
        Ractor.select(@port, @shutdown_port)
      end

      def send_ack
        send_control(RactorPort::Ack.new)
        nil
      rescue StandardError => error
        build_error(:ack_transfer, error)
      end

      def send_cancel
        @state_mutex.synchronize { @cancel_sent = true }
        send_control(RactorPort::Cancel.new(:closed))
        nil
      rescue StandardError => error
        build_error(:cancel_transfer, error)
      end

      def send_control(message)
        if @ack_transfer == :move
          @ack_port.send(message, move: true)
        else
          @ack_port.send(message)
        end
      end

      def build_error(kind, error)
        RactorPortSourceError.new(
          kind: kind,
          cause_class_name: error.class.name,
          cause_message: error.message,
          cause: error
        )
      end

      def closed?
        @state_mutex.synchronize { @closed }
      end

      def wake_coordinator
        close_demand_queue
        return unless @shutdown_port

        @shutdown_port.send(:shutdown)
      rescue StandardError
        nil
      end

      def wait_for_coordinator
        return unless @coordinator

        @coordinator.join
      end

      def deliver_result(result)
        return if @results.closed?

        @results.push(result)
      rescue ClosedQueueError, ThreadError
        nil
      end

      def close_demand_queue
        @demands.close
      end

      def close_result_queue
        @results.close
      end
    end
  end
end
