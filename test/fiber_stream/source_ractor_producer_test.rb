# frozen_string_literal: true

require "async"
require "timeout"
require_relative "../test_helper"

module FiberStream
  class SourceRactorProducerTest < Minitest::Test
    PRODUCE_VALUES =
      Ractor.shareable_proc do |producer, values|
        values.each do |value|
          break unless producer.emit(value)
        end
      end

    PRODUCE_AND_REPORT_CANCEL =
      Ractor.shareable_proc do |producer, values, report_port|
        emitted = 0
        values.each do |value|
          break unless producer.emit(value)

          emitted += 1
        end
        report_port.send([:finished, emitted, producer.cancelled?])
      end

    RAISE_PRODUCER =
      Ractor.shareable_proc do |_producer|
        raise "producer boom"
      end

    FAIL_PRODUCER =
      Ractor.shareable_proc do |producer|
        producer.fail(cause_class_name: "CustomFailure", cause_message: "manual boom")
      end

    INVALID_FAIL_PRODUCER =
      Ractor.shareable_proc do |producer|
        producer.fail
      end

    REPORT_STARTED =
      Ractor.shareable_proc do |producer, report_port|
        report_port.send(:started)
        producer.complete
      end

    DELAYED_PRODUCER =
      Ractor.shareable_proc do |producer, value|
        sleep 0.05
        producer.emit(value)
      end

    def test_ractor_producer_requires_block
      error = assert_raises(ArgumentError) { Source.ractor_producer }

      assert_match(/missing block/, error.message)
    end

    def test_ractor_producer_rejects_unshareable_block
      captured = []

      error = assert_raises(TypeError) do
        Source.ractor_producer { |producer| captured << producer }
      end

      assert_match(/block must be shareable/, error.message)
    end

    def test_ractor_producer_rejects_invalid_transfer
      error = assert_raises(ArgumentError) do
        Source.ractor_producer(transfer: :share, &PRODUCE_VALUES)
      end

      assert_match(/transfer must be :copy or :move/, error.message)
    end

    def test_ractor_producer_is_lazy
      report_port = Ractor::Port.new

      result =
        Source.ractor_producer(report_port, &REPORT_STARTED)
          .take(0)
          .run_with(Sink.to_a)

      assert_empty result
      assert_raises(Timeout::Error) { Timeout.timeout(0.05) { report_port.receive } }
    end

    def test_ractor_producer_emits_values_with_backpressure
      result =
        Source.ractor_producer([1, 2, 3], &PRODUCE_VALUES)
          .map { |value| value * 2 }
          .run_with(Sink.to_a)

      assert_equal [2, 4, 6], result
    end

    def test_ractor_producer_early_close_cancels_owned_producer
      report_port = Ractor::Port.new

      result =
        Source.ractor_producer([1, 2, 3], report_port, &PRODUCE_AND_REPORT_CANCEL)
          .run_with(Sink.first)

      assert_equal 1, result
      assert_equal [:finished, 1, true], wait_for_port_message(report_port)
    end

    def test_ractor_producer_block_failure_is_source_failure
      error = assert_raises(RactorPortSourceError) do
        Source.ractor_producer(&RAISE_PRODUCER).run_with(Sink.to_a)
      end

      assert_equal :producer_failure, error.kind
      assert_equal "RuntimeError", error.cause_class_name
      assert_equal "producer boom", error.cause_message
    end

    def test_ractor_producer_manual_failure_is_source_failure
      error = assert_raises(RactorPortSourceError) do
        Source.ractor_producer(&FAIL_PRODUCER).run_with(Sink.to_a)
      end

      assert_equal :producer_failure, error.kind
      assert_equal "CustomFailure", error.cause_class_name
      assert_equal "manual boom", error.cause_message
    end

    def test_ractor_producer_context_validation_happens_before_waiting_for_ack
      error = assert_raises(RactorPortSourceError) do
        Source.ractor_producer(&INVALID_FAIL_PRODUCER).run_with(Sink.to_a)
      end

      assert_equal :producer_failure, error.kind
      assert_equal "ArgumentError", error.cause_class_name
      assert_match(/fail requires an error/, error.cause_message)
    end

    def test_ractor_producer_spawn_failure_is_setup_failure
      error = assert_raises(RactorPortSourceError) do
        Source.ractor_producer(-> {}, &PRODUCE_VALUES).run_with(Sink.to_a)
      end

      assert_equal :producer_setup, error.kind
    end

    def test_ractor_producer_unexpected_exit_after_ack_is_source_failure
      with_send_failed_spawner do
        error = assert_raises(RactorPortSourceError) do
          Timeout.timeout(1) { Source.ractor_producer(&DELAYED_PRODUCER).run_with(Sink.first) }
        end

        assert_equal :producer_failure, error.kind
        assert_match(/ack-permitted message/, error.cause_message)
      end
    end

    def test_ractor_producer_same_ack_failure_fallback_does_not_raise
      producer = RactorProducer.new(RaisingDataPort.new, AckPort.new, :copy)

      refute producer.emit(:value)
      assert producer.__send__(:send_failed?)
    end

    def test_ractor_producer_wait_does_not_block_async_reactor
      ticks = 0

      result =
        Sync do |task|
          stream_task = task.async { Source.ractor_producer(1, &DELAYED_PRODUCER).run_with(Sink.first) }
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
    end

    def test_ractor_producer_close_during_setup_cancels_late_setup_producer
      with_delayed_setup_spawner do |report_port|
        source = Source.ractor_producer(&DELAYED_PRODUCER)
        sink =
          Sink.__send__(:new) do |stream|
            puller = Thread.new { stream.next }
            sleep 0.01

            stream.close

            assert_equal Pull.const_get(:DONE), puller.value
          end

        Timeout.timeout(1) { source.run_with(sink) }

        assert_equal :cancelled, wait_for_port_message(report_port)
      end
    end

    def test_ractor_producer_close_while_delegate_is_being_installed_does_not_hang
      report_port = Ractor::Port.new

      with_delayed_delegate_install do
        source = Source.ractor_producer([1, 2, 3], report_port, &PRODUCE_AND_REPORT_CANCEL)
        sink =
          Sink.__send__(:new) do |stream|
            puller = Thread.new { stream.next }
            wait_for_port_message(delegate_build_started_port)

            stream.close

            assert_equal Pull.const_get(:DONE), puller.value
          end

        Timeout.timeout(1) { source.run_with(sink) }

        assert_equal [:finished, 0, true], wait_for_port_message(report_port)
      end
    end

    def test_ractor_merge_producers_close_after_delegate_install_before_start_cancels_producers
      report_a = Ractor::Port.new
      report_b = Ractor::Port.new

      with_delayed_delegate_install do
        source =
          Source.ractor_merge_producers do |group|
            group.producer([1, 2, 3], report_a, &PRODUCE_AND_REPORT_CANCEL)
            group.producer([4, 5, 6], report_b, &PRODUCE_AND_REPORT_CANCEL)
          end
        sink =
          Sink.__send__(:new) do |stream|
            puller = Thread.new { stream.next }
            wait_for_port_message(delegate_build_started_port)

            stream.close

            assert_equal Pull.const_get(:DONE), puller.value
          end

        Timeout.timeout(1) { source.run_with(sink) }

        assert_equal [:finished, 0, true], wait_for_port_message(report_a)
        assert_equal [:finished, 0, true], wait_for_port_message(report_b)
      end
    end

    def test_ractor_merge_producers_requires_block
      error = assert_raises(ArgumentError) { Source.ractor_merge_producers }

      assert_match(/missing block/, error.message)
    end

    def test_ractor_merge_producers_requires_at_least_two_producers
      error = assert_raises(ArgumentError) do
        Source.ractor_merge_producers do |group|
          group.producer([1], &PRODUCE_VALUES)
        end
      end

      assert_match(/at least two producers/, error.message)
    end

    def test_ractor_merge_producers_rejects_invalid_producer_transfer
      error = assert_raises(ArgumentError) do
        Source.ractor_merge_producers do |group|
          group.producer([1], transfer: :share, &PRODUCE_VALUES)
          group.producer([2], &PRODUCE_VALUES)
        end
      end

      assert_match(/transfer must be :copy or :move/, error.message)
    end

    def test_ractor_merge_producers_rejects_missing_producer_block
      error = assert_raises(ArgumentError) do
        Source.ractor_merge_producers do |group|
          group.producer([1])
          group.producer([2], &PRODUCE_VALUES)
        end
      end

      assert_match(/missing block/, error.message)
    end

    def test_ractor_merge_producers_partial_setup_failure_cancels_started_producers
      report_port = Ractor::Port.new

      error = assert_raises(RactorPortSourceError) do
        Source.ractor_merge_producers do |group|
          group.producer([1, 2, 3], report_port, &PRODUCE_AND_REPORT_CANCEL)
          group.producer(-> {}, &PRODUCE_VALUES)
        end.run_with(Sink.to_a)
      end

      assert_equal :producer_setup, error.kind
      assert_equal [:finished, 0, true], wait_for_port_message(report_port)
    end

    def test_ractor_merge_producers_setup_thread_failure_cancels_unready_peer
      with_setup_exit_and_delayed_peer_spawner do |report_port|
        error = assert_raises(RactorPortSourceError) do
          Timeout.timeout(1) do
            Source.ractor_merge_producers do |group|
              group.producer(:exit_before_setup, &DELAYED_PRODUCER)
              group.producer(:delayed_setup, &DELAYED_PRODUCER)
            end.run_with(Sink.to_a)
          end
        end

        assert_equal :producer_setup, error.kind
        assert_equal :cancelled, wait_for_port_message(report_port)
      end
    end

    def test_ractor_merge_producers_emits_values_from_all_producers
      result =
        Source.ractor_merge_producers do |group|
          group.producer([[:a, 1], [:a, 2]], &PRODUCE_VALUES)
          group.producer([[:b, 1], [:b, 2]], &PRODUCE_VALUES)
        end.run_with(Sink.to_a)

      assert_equal [[:a, 1], [:a, 2]], (result.select { |side, _| side == :a })
      assert_equal [[:b, 1], [:b, 2]], (result.select { |side, _| side == :b })
    end

    private

    def wait_for_port_message(port)
      Timeout.timeout(1) { port.receive }
    end

    def delegate_build_started_port
      self.class.const_get(:DELEGATE_BUILD_STARTED_PORT)
    end

    def with_delayed_delegate_install
      producer_source = Pull.const_get(:RactorProducerSource)
      original = producer_source.instance_method(:build_delegate)
      port = Ractor::Port.new
      self.class.const_set(:DELEGATE_BUILD_STARTED_PORT, port)

      remove_build_delegate(producer_source)
      producer_source.define_method(:build_delegate) do |producers|
        FiberStream::SourceRactorProducerTest
          .const_get(:DELEGATE_BUILD_STARTED_PORT)
          .send(:started)
        sleep 0.05
        original.bind_call(self, producers)
      end
      producer_source.__send__(:private, :build_delegate)

      yield
    ensure
      remove_build_delegate(producer_source)
      producer_source.define_method(:build_delegate, original)
      producer_source.__send__(:private, :build_delegate)
      self.class.__send__(:remove_const, :DELEGATE_BUILD_STARTED_PORT)
    end

    def with_delayed_setup_spawner
      report_port = Ractor::Port.new
      producer_source = Pull.const_get(:RactorProducerSource)
      original = producer_source.__send__(:method, :spawn_producer)

      remove_spawn_producer(producer_source)
      producer_source.define_singleton_method(:spawn_producer) do |_data_port, setup_port, _definition|
        Ractor.new(setup_port, report_port) do |setup, report|
          sleep 0.05
          ack_port = Ractor::Port.new
          setup.send(ack_port)

          case ack_port.receive
          in FiberStream::RactorPort::Cancel
            report.send(:cancelled)
            FiberStream.const_get(:Pull).const_get(:ProducerCancelled).new
          end
        end
      end
      producer_source.__send__(:private_class_method, :spawn_producer)

      yield report_port
    ensure
      remove_spawn_producer(producer_source)
      producer_source.define_singleton_method(:spawn_producer) do |data_port, setup_port, definition|
        original.call(data_port, setup_port, definition)
      end
      producer_source.__send__(:private_class_method, :spawn_producer)
    end

    def with_send_failed_spawner
      producer_source = Pull.const_get(:RactorProducerSource)
      original = producer_source.__send__(:method, :spawn_producer)

      remove_spawn_producer(producer_source)
      producer_source.define_singleton_method(:spawn_producer) do |_data_port, setup_port, _definition|
        Ractor.new(setup_port) do |setup|
          ack_port = Ractor::Port.new
          setup.send(ack_port)
          ack_port.receive
          FiberStream.const_get(:Pull).const_get(:ProducerSendFailed).new
        end
      end
      producer_source.__send__(:private_class_method, :spawn_producer)

      yield
    ensure
      remove_spawn_producer(producer_source)
      producer_source.define_singleton_method(:spawn_producer) do |data_port, setup_port, definition|
        original.call(data_port, setup_port, definition)
      end
      producer_source.__send__(:private_class_method, :spawn_producer)
    end

    def with_setup_exit_and_delayed_peer_spawner
      report_port = Ractor::Port.new
      producer_source = Pull.const_get(:RactorProducerSource)
      original = producer_source.__send__(:method, :spawn_producer)

      remove_spawn_producer(producer_source)
      producer_source.define_singleton_method(:spawn_producer) do |_data_port, setup_port, definition|
        case definition.args.fetch(0)
        when :exit_before_setup
          Ractor.new { :setup_exit }
        when :delayed_setup
          Ractor.new(setup_port, report_port) do |setup, report|
            sleep 0.05
            ack_port = Ractor::Port.new
            setup.send(ack_port)

            case ack_port.receive
            in FiberStream::RactorPort::Cancel
              report.send(:cancelled)
              FiberStream.const_get(:Pull).const_get(:ProducerCancelled).new
            end
          end
        end
      end
      producer_source.__send__(:private_class_method, :spawn_producer)

      yield report_port
    ensure
      remove_spawn_producer(producer_source)
      producer_source.define_singleton_method(:spawn_producer) do |data_port, setup_port, definition|
        original.call(data_port, setup_port, definition)
      end
      producer_source.__send__(:private_class_method, :spawn_producer)
    end

    def remove_spawn_producer(producer_source)
      producer_source.singleton_class.__send__(:remove_method, :spawn_producer)
    rescue NameError
      nil
    end

    def remove_build_delegate(producer_source)
      producer_source.__send__(:remove_method, :build_delegate)
    rescue NameError
      nil
    end

    class AckPort
      def receive
        RactorPort::Ack.new
      end
    end

    class RaisingDataPort
      def send(_message, move: false)
        raise "unexpected move transfer" if move

        raise "send boom"
      end
    end
  end
end
