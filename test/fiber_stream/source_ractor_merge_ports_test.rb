# frozen_string_literal: true

require "async"
require "timeout"
require_relative "../test_helper"

module FiberStream
  class SourceRactorMergePortsTest < Minitest::Test
    def test_ractor_merge_ports_is_lazy
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      ack_a = RecordingPort.new
      ack_b = RecordingPort.new

      Source.ractor_merge_ports(
        [
          { port: port_a, ack_port: ack_a },
          { port: port_b, ack_port: ack_b }
        ]
      )

      assert_empty ack_a.messages
      assert_empty ack_b.messages
    end

    def test_ractor_merge_ports_rejects_less_than_two_pairs
      error = assert_raises(ArgumentError) do
        Source.ractor_merge_ports([{ port: Ractor::Port.new, ack_port: Ractor::Port.new }])
      end

      assert_match(/at least two port pairs/, error.message)
    end

    def test_ractor_merge_ports_rejects_invalid_pair
      error = assert_raises(TypeError) do
        Source.ractor_merge_ports([Object.new, { port: Ractor::Port.new, ack_port: Ractor::Port.new }])
      end

      assert_match(/port pair must be a Hash/, error.message)
    end

    def test_ractor_merge_ports_rejects_duplicate_data_ports
      data_port = Ractor::Port.new

      error = assert_raises(ArgumentError) do
        Source.ractor_merge_ports(
          [
            { port: data_port, ack_port: Ractor::Port.new },
            { port: data_port, ack_port: Ractor::Port.new }
          ]
        )
      end

      assert_match(/data ports must be distinct/, error.message)
    end

    def test_ractor_merge_ports_rejects_duplicate_ack_ports
      ack_port = Ractor::Port.new

      error = assert_raises(ArgumentError) do
        Source.ractor_merge_ports(
          [
            { port: Ractor::Port.new, ack_port: ack_port },
            { port: Ractor::Port.new, ack_port: ack_port }
          ]
        )
      end

      assert_match(/ack ports must be distinct/, error.message)
    end

    def test_ractor_merge_ports_does_not_start_when_downstream_completes_before_demand
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      ack_a = RecordingPort.new
      ack_b = RecordingPort.new

      result =
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ]
        ).take(0).run_with(Sink.to_a)

      assert_empty result
      assert_empty ack_a.messages
      assert_empty ack_b.messages
    end

    def test_ractor_merge_ports_emits_values_from_all_producers
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      producer_a, ack_a = counting_producer(port_a, [[:a, 1], [:a, 2]])
      producer_b, ack_b = counting_producer(port_b, [[:b, 1], [:b, 2]])

      result =
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ]
        ).run_with(Sink.to_a)

      assert_equal [[:a, 1], [:a, 2]], (result.select { |side, _| side == :a })
      assert_equal [[:b, 1], [:b, 2]], (result.select { |side, _| side == :b })
      assert_equal [:completed, 3], wait_for_ractor_value(producer_a)
      assert_equal [:completed, 3], wait_for_ractor_value(producer_b)
    end

    def test_ractor_merge_ports_continues_after_one_producer_completes
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      producer_a, ack_a = counting_producer(port_a, [1])
      producer_b, ack_b = counting_producer(port_b, [2, 3])

      result =
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ]
        ).run_with(Sink.to_a)

      assert_equal [1, 2, 3], result.sort
      assert_equal [:completed, 2], wait_for_ractor_value(producer_a)
      assert_equal [:completed, 3], wait_for_ractor_value(producer_b)
    end

    def test_ractor_merge_ports_sends_one_initial_ack_per_producer
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      ack_a = RecordingPort.new
      ack_b = RecordingPort.new
      port_a.send(RactorPort::Complete.new)
      port_b.send(RactorPort::Complete.new)

      result =
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ]
        ).run_with(Sink.to_a)

      assert_empty result
      assert_equal [RactorPort::Ack.new], ack_a.messages
      assert_equal [RactorPort::Ack.new], ack_b.messages
    end

    def test_ractor_merge_ports_early_completion_cancels_non_terminal_producers
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      producer_a, ack_a = counting_producer(port_a, [1, 2, 3])
      producer_b, ack_b = counting_producer(port_b, [4, 5, 6])

      result =
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ]
        ).run_with(Sink.first)

      refute_nil result
      assert_cancelled(wait_for_ractor_value(producer_a))
      assert_cancelled(wait_for_ractor_value(producer_b))
    end

    def test_ractor_merge_ports_producer_failure_suppresses_cancel_for_failed_side
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      ack_a = RecordingPort.new
      producer_b, ack_b = waiting_producer(port_b)
      port_a.send(RactorPort::Failure.new("RuntimeError", "producer boom"))

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ]
        ).run_with(Sink.to_a)
      end

      assert_equal :producer_failure, error.kind
      assert_equal [RactorPort::Ack.new], ack_a.messages
      assert_equal RactorPort::Cancel.new(:closed), wait_for_ractor_value(producer_b)
    end

    def test_ractor_merge_ports_invalid_message_cancels_invalid_side
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      ack_a = RecordingPort.new
      ack_b = RecordingPort.new
      port_a.send(:raw_value)

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ]
        ).run_with(Sink.to_a)
      end

      assert_equal :invalid_message, error.kind
      assert_equal [RactorPort::Ack.new, RactorPort::Cancel.new(:closed)], ack_a.messages
      assert_equal [RactorPort::Ack.new, RactorPort::Cancel.new(:closed)], ack_b.messages
    end

    def test_ractor_merge_ports_malformed_failure_payload_fails_stream
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      ack_a = RecordingPort.new
      ack_b = RecordingPort.new
      port_a.send(RactorPort::Failure.new(:runtime_error, "producer boom"))

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ]
        ).run_with(Sink.to_a)
      end

      assert_equal :invalid_message, error.kind
      assert_equal "FiberStream::RactorPort::Failure", error.cause_class_name
    end

    def test_ractor_merge_ports_receive_failure_is_normalized
      port = ObjectWithReceive.new
      ack_a = RecordingPort.new
      ack_b = RecordingPort.new

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_merge_ports(
          [
            { port: port, ack_port: ack_a },
            { port: Ractor::Port.new, ack_port: ack_b }
          ]
        ).run_with(Sink.first)
      end

      assert_equal :receive, error.kind
    end

    def test_ractor_merge_ports_ack_send_failure_is_stream_failure
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      ack_a = RaisingPort.new(raise_on: RactorPort::Ack)
      ack_b = RecordingPort.new

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ]
        ).run_with(Sink.first)
      end

      assert_equal :ack_transfer, error.kind
      assert_equal "send boom", error.cause_message
    end

    def test_ractor_merge_ports_cancel_send_failure_is_close_failure
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      ack_a = RaisingPort.new(raise_on: RactorPort::Cancel)
      ack_b = RecordingPort.new
      port_a.send(RactorPort::Element.new(1))

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ]
        ).run_with(Sink.first)
      end

      assert_equal :cancel_transfer, error.kind
      assert_equal "send boom", error.cause_message
    end

    def test_ractor_merge_ports_cancel_false_sends_no_cancel
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      ack_a = RecordingPort.new
      ack_b = RecordingPort.new
      port_a.send(RactorPort::Element.new(1))

      result =
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ],
          cancel: false
        ).run_with(Sink.first)

      assert_equal 1, result
      assert_equal [RactorPort::Ack.new], ack_a.messages
      assert_equal [RactorPort::Ack.new], ack_b.messages
    end

    def test_ractor_merge_ports_ack_transfer_move_sends_fresh_control_messages
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      ack_a = MoveRecordingPort.new
      ack_b = MoveRecordingPort.new
      port_a.send(RactorPort::Element.new(1))

      Source.ractor_merge_ports(
        [
          { port: port_a, ack_port: ack_a },
          { port: port_b, ack_port: ack_b }
        ],
        ack_transfer: :move
      ).run_with(Sink.first)

      assert_equal [[RactorPort::Ack.new, true], [RactorPort::Cancel.new(:closed), true]], ack_a.messages
      assert_equal [[RactorPort::Ack.new, true], [RactorPort::Cancel.new(:closed), true]], ack_b.messages
      refute_same ack_a.messages.fetch(0).fetch(0), ack_b.messages.fetch(0).fetch(0)
      refute_same ack_a.messages.fetch(1).fetch(0), ack_b.messages.fetch(1).fetch(0)
    end

    def test_ractor_merge_ports_does_not_require_scheduler
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      ack_a = RecordingPort.new
      ack_b = RecordingPort.new
      port_a.send(RactorPort::Complete.new)
      port_b.send(RactorPort::Complete.new)

      result =
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ]
        ).run_with(Sink.to_a)

      assert_empty result
    end

    def test_ractor_merge_ports_wait_does_not_block_async_reactor
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      producer_a, ack_a = delayed_producer(port_a, 1)
      producer_b, ack_b = delayed_producer(port_b, 2)
      ticks = 0

      result =
        Sync do |task|
          stream_task =
            task.async do
              Source.ractor_merge_ports(
                [
                  { port: port_a, ack_port: ack_a },
                  { port: port_b, ack_port: ack_b }
                ]
              ).run_with(Sink.first)
            end
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

      assert_includes [1, 2], result.fetch(0)
      assert_operator result.fetch(1), :>=, 1
      assert_operator ticks, :>=, 3
      assert_equal RactorPort::Cancel.new(:closed), wait_for_ractor_value(producer_a)
      assert_equal RactorPort::Cancel.new(:closed), wait_for_ractor_value(producer_b)
    end

    def test_ractor_merge_ports_close_while_result_is_outstanding_does_not_hang
      port_a = Ractor::Port.new
      port_b = Ractor::Port.new
      ack_a = RecordingPort.new
      ack_b = RecordingPort.new
      source =
        Source.ractor_merge_ports(
          [
            { port: port_a, ack_port: ack_a },
            { port: port_b, ack_port: ack_b }
          ]
        )
      sink =
        Sink.__send__(:new) do |stream|
          puller = Thread.new { stream.next }
          wait_until do
            ack_a.messages.include?(RactorPort::Ack.new) &&
              ack_b.messages.include?(RactorPort::Ack.new)
          end

          stream.close

          assert_equal FiberStream.const_get(:Pull).const_get(:DONE), puller.value
        end

      Timeout.timeout(1) { source.run_with(sink) }

      assert_equal [RactorPort::Ack.new, RactorPort::Cancel.new(:closed)], ack_a.messages
      assert_equal [RactorPort::Ack.new, RactorPort::Cancel.new(:closed)], ack_b.messages
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

    def waiting_producer(data_port)
      setup_port = Ractor::Port.new
      producer =
        Ractor.new(data_port, setup_port) do |_outbox, setup|
          inbox = Ractor::Port.new
          setup.send(inbox)

          loop do
            case inbox.receive
            in FiberStream::RactorPort::Ack
              next
            in FiberStream::RactorPort::Cancel[reason]
              break FiberStream::RactorPort::Cancel.new(reason)
            end
          end
        end

      [producer, setup_port.receive]
    end

    def delayed_producer(data_port, value)
      setup_port = Ractor::Port.new
      producer =
        Ractor.new(data_port, setup_port, value) do |outbox, setup, element|
          inbox = Ractor::Port.new
          setup.send(inbox)

          case inbox.receive
          in FiberStream::RactorPort::Ack
            sleep 0.05
            outbox.send(FiberStream::RactorPort::Element.new(element))
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

    def assert_cancelled(value)
      assert_equal :cancelled, value.fetch(0)
      assert_operator value.fetch(1), :>=, 1
      assert_equal :closed, value.fetch(2)
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
        @mutex = Mutex.new
      end

      def send(message, move: false)
        @mutex.synchronize { @messages << [message, move] }
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
