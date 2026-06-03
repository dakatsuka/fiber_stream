# frozen_string_literal: true

require "async"
require "timeout"
require_relative "../test_helper"

module FiberStream
  class SourceRactorPortTest < Minitest::Test
    def test_ractor_port_is_lazy
      port = Ractor::Port.new
      ack_port = RecordingPort.new

      Source.ractor_port(port, ack_port: ack_port)

      assert_empty ack_port.messages
    end

    def test_ractor_port_rejects_invalid_data_port
      error = assert_raises(TypeError) do
        Source.ractor_port(Object.new, ack_port: Ractor::Port.new)
      end

      assert_match(/port must respond to receive/, error.message)
    end

    def test_ractor_port_rejects_invalid_ack_port
      error = assert_raises(TypeError) do
        Source.ractor_port(Ractor::Port.new, ack_port: Object.new)
      end

      assert_match(/ack_port must provide Ractor-style send/, error.message)
    end

    def test_ractor_port_rejects_invalid_ack_transfer
      error = assert_raises(ArgumentError) do
        Source.ractor_port(Ractor::Port.new, ack_port: Ractor::Port.new, ack_transfer: :share)
      end

      assert_match(/ack_transfer must be :copy or :move/, error.message)
    end

    def test_ractor_port_rejects_invalid_cancel
      error = assert_raises(TypeError) do
        Source.ractor_port(Ractor::Port.new, ack_port: Ractor::Port.new, cancel: nil)
      end

      assert_match(/cancel must be true or false/, error.message)
    end

    def test_ractor_port_consumes_elements_with_ack_handshake
      data_port = Ractor::Port.new
      producer, ack_port = counting_producer(data_port, [1, 2, 3])

      result =
        Source.ractor_port(data_port, ack_port: ack_port)
          .map { |value| value * 2 }
          .run_with(Sink.to_a)

      assert_equal [2, 4, 6], result
      assert_equal [:completed, 4], wait_for_ractor_value(producer)
    end

    def test_ractor_port_sends_one_ack_per_downstream_demand
      data_port = Ractor::Port.new
      producer, ack_port = counting_producer(data_port, [1, 2, 3])

      result =
        Source.ractor_port(data_port, ack_port: ack_port)
          .run_with(Sink.first)

      assert_equal 1, result
      assert_equal [:cancelled, 1, :closed], wait_for_ractor_value(producer)
    end

    def test_ractor_port_complete_suppresses_cancel_on_cleanup
      data_port = Ractor::Port.new
      ack_port = RecordingPort.new
      data_port.send(RactorPort::Complete.new)

      result = Source.ractor_port(data_port, ack_port: ack_port).run_with(Sink.to_a)

      assert_empty result
      assert_equal [RactorPort::Ack.new], ack_port.messages
    end

    def test_ractor_port_producer_failure_suppresses_cancel_on_cleanup
      data_port = Ractor::Port.new
      ack_port = RecordingPort.new
      data_port.send(RactorPort::Failure.new("RuntimeError", "producer boom"))

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_port(data_port, ack_port: ack_port).run_with(Sink.to_a)
      end

      assert_equal :producer_failure, error.kind
      assert_equal "RuntimeError", error.cause_class_name
      assert_equal "producer boom", error.cause_message
      assert_equal [RactorPort::Ack.new], ack_port.messages
    end

    def test_ractor_port_invalid_message_fails_stream
      data_port = Ractor::Port.new
      ack_port = RecordingPort.new
      data_port.send(:raw_value)

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_port(data_port, ack_port: ack_port).run_with(Sink.to_a)
      end

      assert_equal :invalid_message, error.kind
      assert_equal "Symbol", error.cause_class_name
      assert_equal [RactorPort::Ack.new, RactorPort::Cancel.new(:closed)], ack_port.messages
    end

    def test_ractor_port_malformed_failure_payload_fails_stream
      data_port = Ractor::Port.new
      ack_port = RecordingPort.new
      data_port.send(RactorPort::Failure.new(:runtime_error, "producer boom"))

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_port(data_port, ack_port: ack_port).run_with(Sink.to_a)
      end

      assert_equal :invalid_message, error.kind
      assert_equal "FiberStream::RactorPort::Failure", error.cause_class_name
    end

    def test_ractor_port_ack_send_failure_is_stream_failure
      data_port = Ractor::Port.new
      ack_port = RaisingPort.new(raise_on: RactorPort::Ack)

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_port(data_port, ack_port: ack_port).run_with(Sink.first)
      end

      assert_equal :ack_transfer, error.kind
      assert_equal "RuntimeError", error.cause_class_name
      assert_equal "send boom", error.cause_message
    end

    def test_ractor_port_cancel_send_failure_is_close_failure
      data_port = Ractor::Port.new
      ack_port = RaisingPort.new(raise_on: RactorPort::Cancel)
      data_port.send(RactorPort::Element.new(1))

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_port(data_port, ack_port: ack_port).run_with(Sink.first)
      end

      assert_equal :cancel_transfer, error.kind
      assert_equal "RuntimeError", error.cause_class_name
      assert_equal "send boom", error.cause_message
    end

    def test_ractor_port_cancel_false_sends_no_cancel_on_early_close
      data_port = Ractor::Port.new
      ack_port = RecordingPort.new
      data_port.send(RactorPort::Element.new(1))

      result =
        Source.ractor_port(data_port, ack_port: ack_port, cancel: false)
          .run_with(Sink.first)

      assert_equal 1, result
      assert_equal [RactorPort::Ack.new], ack_port.messages
    end

    def test_ractor_port_ack_transfer_move_sends_control_with_move
      data_port = Ractor::Port.new
      ack_port = MoveRecordingPort.new
      data_port.send(RactorPort::Complete.new)

      Source.ractor_port(data_port, ack_port: ack_port, ack_transfer: :move)
        .run_with(Sink.to_a)

      assert_equal [[RactorPort::Ack.new, true]], ack_port.messages
    end

    def test_ractor_port_downstream_failure_suppresses_cancel_send_failure
      data_port = Ractor::Port.new
      ack_port = RaisingPort.new(raise_on: RactorPort::Cancel)
      data_port.send(RactorPort::Element.new(1))
      sink =
        Sink.__send__(:new) do |stream|
          stream.next
          raise "sink boom"
        end

      error = assert_raises(RuntimeError) do
        Source.ractor_port(data_port, ack_port: ack_port).run_with(sink)
      end

      assert_equal "sink boom", error.message
    end

    def test_ractor_port_receive_failure_is_normalized
      port = ObjectWithReceive.new
      ack_port = RecordingPort.new

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_port(port, ack_port: ack_port).run_with(Sink.first)
      end

      assert_equal :receive, error.kind
      assert_equal "ArgumentError", error.cause_class_name
    end

    def test_ractor_port_close_while_ack_is_outstanding_does_not_hang
      data_port = Ractor::Port.new
      ack_port = RecordingPort.new
      source = Source.ractor_port(data_port, ack_port: ack_port)
      sink =
        Sink.__send__(:new) do |stream|
          puller = Thread.new { stream.next }
          wait_until { ack_port.messages.include?(RactorPort::Ack.new) }

          stream.close

          assert_equal FiberStream.const_get(:Pull).const_get(:DONE), puller.value
        end

      Timeout.timeout(1) { source.run_with(sink) }

      assert_equal [RactorPort::Ack.new, RactorPort::Cancel.new(:closed)], ack_port.messages
    end

    def test_ractor_port_wait_does_not_block_async_reactor
      data_port = Ractor::Port.new
      producer, ack_port = delayed_producer(data_port)
      ticks = 0

      result =
        Sync do |task|
          stream_task = task.async { Source.ractor_port(data_port, ack_port: ack_port).run_with(Sink.first) }
          ticker =
            task.async do
              3.times do
                sleep 0.01
                ticks += 1
              end
            end

          sleep 0.02
          ticks_while_stream_waited = ticks
          value = stream_task.wait

          ticker.wait
          [value, ticks_while_stream_waited]
        end

      assert_equal 1, result.fetch(0)
      assert_operator result.fetch(1), :>=, 1
      assert_operator ticks, :>=, 3
      assert_equal RactorPort::Cancel.new(:closed), wait_for_ractor_value(producer)
    end

    private

    def counting_producer(data_port, values)
      setup_port = Ractor::Port.new
      producer =
        Ractor.new(data_port, setup_port, values) do |outbox, setup, producer_values|
          inbox = Ractor::Port.new
          setup.send(inbox)
          enumerator = producer_values.to_enum
          ack_count = 0

          loop do
            case inbox.receive
            in FiberStream::RactorPort::Ack
              ack_count += 1
              begin
                outbox.send(FiberStream::RactorPort::Element.new(enumerator.next))
              rescue StopIteration
                outbox.send(FiberStream::RactorPort::Complete.new)
                break [:completed, ack_count]
              end
            in FiberStream::RactorPort::Cancel[reason]
              break [:cancelled, ack_count, reason]
            end
          end
        end

      [producer, setup_port.receive]
    end

    def delayed_producer(data_port)
      setup_port = Ractor::Port.new
      producer =
        Ractor.new(data_port, setup_port) do |outbox, setup|
          inbox = Ractor::Port.new
          setup.send(inbox)

          case inbox.receive
          in FiberStream::RactorPort::Ack
            sleep 0.05
            outbox.send(FiberStream::RactorPort::Element.new(1))
            inbox.receive
          end
        end

      [producer, setup_port.receive]
    end

    def wait_for_ractor_value(ractor)
      Timeout.timeout(1) { ractor.value }
    end

    def wait_until
      Timeout.timeout(1) do
        sleep 0.001 until yield
      end
    end

    class RecordingPort
      attr_reader :messages

      def initialize
        @messages = []
        @mutex = Mutex.new
      end

      def send(message, move: false)
        raise "unexpected move transfer" if move

        @mutex.synchronize { @messages << message }
      end
    end

    class MoveRecordingPort
      attr_reader :messages

      def initialize
        @messages = []
      end

      def send(message, move: false)
        @messages << [message, move]
      end
    end

    class RaisingPort
      def initialize(raise_on:)
        @raise_on = raise_on
      end

      def send(message, move: false)
        raise "unexpected move transfer" if move
        raise "send boom" if message.is_a?(@raise_on)

        message
      end
    end

    class ObjectWithReceive
      def receive
        raise "unused"
      end
    end
  end
end
