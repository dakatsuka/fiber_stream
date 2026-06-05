# frozen_string_literal: true

module FiberStream
  module Pull
    # Pull stream for `Source.ractor_merge_ports`.
    #
    # A coordinator thread owns blocking Ractor waits and forwards producer
    # messages through a bounded result mailbox. Each producer receives at most
    # one outstanding ack, and downstream demand replenishes only the producer
    # that emitted the previous value.
    class RactorMergePortsSource
      WAIT_INTERVAL = 0.001
      PortPair = Data.define(:side, :port, :ack_port)
      StartCommand = Data.define
      RequestAckCommand = Data.define(:side)
      ShutdownCommand = Data.define
      ValueResult = Data.define(:side, :value)
      DoneResult = Data.define(:side)
      ErrorResult = Data.define(:side, :error)
      private_constant :PortPair, :StartCommand, :RequestAckCommand, :ShutdownCommand
      private_constant :ValueResult, :DoneResult, :ErrorResult

      def initialize(port_pairs, ack_transfer, cancel)
        @pairs = port_pairs.each_with_index.map do |pair, side|
          PortPair.new(side:, port: pair.port, ack_port: pair.ack_port)
        end.freeze
        @ack_transfer = ack_transfer
        @cancel_enabled = cancel
        @result_mailbox = RactorMergeResultMailbox.new(@pairs.size)
        @control_port = nil
        @coordinator = nil
        @state_mutex = Mutex.new
        @producer_terminal = @pairs.to_h { |pair| [pair.side, false] }
        @side_done = @pairs.to_h { |pair| [pair.side, false] }
        @cancel_sent = @pairs.to_h { |pair| [pair.side, false] }
        @pending_ack_sides = {}
        @started = false
        @closed = false
        @done = false
      end

      def next
        return DONE if closed_or_done?

        start
        request_pending_acks
        next_result
      end

      def close
        started = mark_closed
        return if started.nil?
        return unless started

        close_result_mailbox
        wake_coordinator
        wait_for_coordinator
        cancel_error = cancel_non_terminal_producers
        raise cancel_error if cancel_error
      end

      private

      def closed_or_done?
        @state_mutex.synchronize { @closed || @done }
      end

      def mark_closed
        already_closed = false
        started = false

        @state_mutex.synchronize do
          already_closed = @closed
          started = @started
          @closed = true
          @done = true
        end

        already_closed ? nil : started
      end

      def start
        started_now = false

        @state_mutex.synchronize do
          return if @started || @closed

          @control_port = Ractor::Port.new
          @started = true
          @coordinator = Thread.new { run_coordinator }
          started_now = true
        end

        send_control_command(StartCommand.new) if started_now && !closed_or_done?
      end

      def request_pending_acks
        sides =
          @state_mutex.synchronize do
            pending = @pending_ack_sides.keys
            @pending_ack_sides.clear
            pending
          end

        sides.each { |side| send_control_command(RequestAckCommand.new(side:)) }
      end

      def next_result
        loop do
          result = @result_mailbox.pop
          return complete if result.nil?

          case result
          in ValueResult[side:, value:]
            record_pending_ack(side)
            return value
          in DoneResult[side:]
            mark_side_done(side)
            return complete if all_done?
          in ErrorResult[error:]
            mark_done
            raise_error(error)
          end
        end
      rescue RactorMergeResultMailbox::Closed
        complete
      end

      def record_pending_ack(side)
        @state_mutex.synchronize do
          @pending_ack_sides[side] = true unless @closed || @producer_terminal.fetch(side)
        end
      end

      def mark_side_done(side)
        @state_mutex.synchronize { @side_done[side] = true }
      end

      def all_done?
        @state_mutex.synchronize { @side_done.values.all? }
      end

      def mark_done
        @state_mutex.synchronize { @done = true }
      end

      def complete
        mark_done
        DONE
      end

      def run_coordinator
        outstanding_ack = @pairs.to_h { |pair| [pair.side, false] }
        active_ports = @pairs.map(&:port)
        pair_by_port = @pairs.to_h { |pair| [pair.port, pair] }

        loop do
          break if active_ports.empty?

          selected, message = Ractor.select(@control_port, *active_ports)
          if selected == @control_port
            break if handle_control_message(message, outstanding_ack)
          else
            pair = pair_by_port.fetch(selected)
            outstanding_ack[pair.side] = false
            result = build_result(pair, message)
            active_ports.delete(pair.port) if producer_terminal?(pair.side)
            deliver_result(result)
          end
        end
      rescue StandardError => error
        deliver_result(ErrorResult.new(side: nil, error: build_error(:receive, error)))
      end

      def handle_control_message(message, outstanding_ack)
        case message
        in StartCommand
          @pairs.each { |pair| ack_pair(pair, outstanding_ack) }
          false
        in RequestAckCommand[side:]
          pair = @pairs.fetch(side)
          ack_pair(pair, outstanding_ack)
          false
        in ShutdownCommand
          true
        else
          false
        end
      end

      def ack_pair(pair, outstanding_ack)
        return if outstanding_ack.fetch(pair.side)
        return if producer_terminal?(pair.side)

        ack_error = send_ack(pair)
        if ack_error
          deliver_result(ErrorResult.new(side: pair.side, error: ack_error))
        else
          outstanding_ack[pair.side] = true
        end
      end

      def send_ack(pair)
        send_control(pair.ack_port, RactorPort::Ack.new)
        nil
      rescue StandardError => error
        build_error(:ack_transfer, error)
      end

      def build_result(pair, message)
        side = pair.side

        case message
        in RactorPort::Element[value]
          ValueResult.new(side:, value:)
        in RactorPort::Complete
          mark_producer_terminal(side)
          DoneResult.new(side:)
        in RactorPort::Failure[String => cause_class_name, String => cause_message]
          mark_producer_terminal(side)
          ErrorResult.new(
            side:,
            error: RactorPortSourceError.new(
              kind: :producer_failure,
              cause_class_name: cause_class_name,
              cause_message: cause_message
            )
          )
        in RactorPort::Failure
          ErrorResult.new(side:, error: invalid_message_error(message, "Failure payloads must be Strings"))
        else
          ErrorResult.new(side:, error: invalid_message_error(message, "invalid RactorPort message"))
        end
      end

      def invalid_message_error(message, cause_message)
        RactorPortSourceError.new(
          kind: :invalid_message,
          cause_class_name: message.class.name,
          cause_message: cause_message
        )
      end

      def build_error(kind, error)
        RactorPortSourceError.new(
          kind: kind,
          cause_class_name: error.class.name,
          cause_message: error.message,
          cause: error
        )
      end

      def mark_producer_terminal(side)
        @state_mutex.synchronize { @producer_terminal[side] = true }
      end

      def producer_terminal?(side)
        @state_mutex.synchronize { @producer_terminal.fetch(side) }
      end

      def cancel_non_terminal_producers
        first_error = nil

        @pairs.each do |pair|
          next unless should_cancel_pair?(pair)

          cancel_error = send_cancel(pair)
          first_error ||= cancel_error
        end

        first_error
      end

      def should_cancel_pair?(pair)
        @state_mutex.synchronize do
          return false unless @cancel_enabled
          return false if @producer_terminal.fetch(pair.side)
          return false if @cancel_sent.fetch(pair.side)

          @cancel_sent[pair.side] = true
          true
        end
      end

      def send_cancel(pair)
        send_control(pair.ack_port, RactorPort::Cancel.new(:closed))
        nil
      rescue StandardError => error
        build_error(:cancel_transfer, error)
      end

      def send_control(port, message)
        if @ack_transfer == :move
          port.send(message, move: true)
        else
          port.send(message)
        end
      end

      def raise_error(error)
        if error.is_a?(RactorPortSourceError) && error.original_cause
          raise error, cause: error.original_cause
        end

        raise error
      end

      def deliver_result(result)
        @result_mailbox.push(result)
      rescue RactorMergeResultMailbox::Closed
        nil
      end

      def send_control_command(command)
        @control_port&.send(command)
      rescue StandardError
        nil
      end

      def wake_coordinator
        send_control_command(ShutdownCommand.new)
      end

      def wait_for_coordinator
        return unless @coordinator

        sleep WAIT_INTERVAL while @coordinator.alive?
        @coordinator.join
      end

      def close_result_mailbox
        @result_mailbox.close
      end

      class RactorMergeResultMailbox
        Closed = Class.new(StandardError)

        def initialize(capacity)
          @queue = Thread::SizedQueue.new(capacity)
        end

        def push(result)
          @queue << result
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
