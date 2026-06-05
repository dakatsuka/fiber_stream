# frozen_string_literal: true

require "async"
require "timeout"
require_relative "../test_helper"

module FiberStream
  class FlowRactorMapTest < Minitest::Test
    ORDERED_MAPPER =
      Ractor.shareable_proc do |value|
        sleep 0.02 if value == 1
        value * 10
      end

    IDENTITY_MAPPER = Ractor.shareable_proc { |value| value }

    MUTATING_MAPPER =
      Ractor.shareable_proc do |value|
        value << "!"
        value
      end

    def test_ractor_map_preserves_ordered_values
      result =
        Source.each([1, 2, 3])
          .ractor_map(workers: 2, &ORDERED_MAPPER)
          .run_with(Sink.to_a)

      assert_equal [10, 20, 30], result
    end

    def test_ractor_map_is_lazy_and_does_not_require_scheduler_until_demanded
      pulled = false
      mapper = Ractor.shareable_proc { raise "should not be called" }

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.ractor_map(workers: 2, &mapper))

      refute pulled
    end

    def test_ractor_map_does_not_require_scheduler_when_demanded
      result =
        Source.each([1])
          .ractor_map(workers: 1, &IDENTITY_MAPPER)
          .run_with(Sink.to_a)

      assert_equal [1], result
    end

    def test_ractor_map_requires_block
      error = assert_raises(ArgumentError) do
        Flow.ractor_map(workers: 2)
      end

      assert_match(/missing block/, error.message)
    end

    def test_ractor_map_rejects_non_shareable_block
      ordinary_proc = proc { |value| value }

      error = assert_raises(TypeError) do
        Flow.ractor_map(workers: 2, &ordinary_proc)
      end

      assert_match(/block must be shareable/, error.message)
    end

    def test_ractor_map_rejects_non_integer_workers
      error = assert_raises(TypeError) do
        Flow.ractor_map(workers: 1.5, &IDENTITY_MAPPER)
      end

      assert_match(/workers must be an Integer/, error.message)
    end

    def test_ractor_map_rejects_zero_workers
      error = assert_raises(ArgumentError) do
        Source.each([1]).ractor_map(workers: 0, &IDENTITY_MAPPER)
      end

      assert_match(/workers must be positive/, error.message)
    end

    def test_ractor_map_rejects_invalid_transfer_policy
      error = assert_raises(ArgumentError) do
        Flow.ractor_map(workers: 1, input_transfer: :share, &IDENTITY_MAPPER)
      end

      assert_match(/input_transfer must be :copy or :move/, error.message)
    end

    def test_ractor_map_bounds_pulled_but_unemitted_values
      pulled = 0

      Source.each(1.upto(100))
        .via(build_next_counting_flow { pulled += 1 })
        .ractor_map(workers: 2, &ORDERED_MAPPER)
        .run_with(Sink.first)

      assert_operator pulled, :<=, 3
    end

    def test_ractor_map_delivers_later_failure_after_earlier_value
      observed = []
      mapper =
        Ractor.shareable_proc do |value|
          raise "map boom" if value == 2

          sleep 0.02
          value
        end
      sink =
        Sink.__send__(:new) do |stream|
          observed << stream.next
          stream.next
        end

      error = assert_raises(RactorMapError) do
        Source.each([1, 2])
          .ractor_map(workers: 2, &mapper)
          .run_with(sink)
      end

      assert_equal [1], observed
      assert_equal 1, error.sequence
      assert_equal :worker, error.kind
      assert_equal "RuntimeError", error.cause_class_name
      assert_equal "map boom", error.cause_message
    end

    def test_ractor_map_propagates_upstream_errors_after_earlier_values
      sink =
        Sink.__send__(:new) do |stream|
          [stream.next, stream.next]
        end

      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(Flow.map { |value| explode_after_first(value) })
          .ractor_map(workers: 2, &IDENTITY_MAPPER)
          .run_with(sink)
      end

      assert_equal "upstream boom", error.message
    end

    def test_ractor_map_suppresses_in_flight_mapping_error_after_early_completion
      mapper =
        Ractor.shareable_proc do |value|
          raise "ignored boom" if value == 2

          value
        end

      result =
        Source.each([1, 2])
          .ractor_map(workers: 2, &mapper)
          .run_with(Sink.first)

      assert_equal 1, result
    end

    def test_ractor_map_closes_upstream_after_early_completion
      closed = false

      result =
        Source.each([1, 2, 3])
          .via(build_close_tracking_flow { closed = true })
          .ractor_map(workers: 2, &IDENTITY_MAPPER)
          .run_with(Sink.first)

      assert_equal 1, result
      assert closed
    end

    def test_ractor_map_preserves_close_error_after_early_completion
      error = assert_raises(RuntimeError) do
        Source.each([1, 2, 3])
          .via(build_close_raising_flow)
          .ractor_map(workers: 2, &IDENTITY_MAPPER)
          .run_with(Sink.first)
      end

      assert_equal "close boom", error.message
    end

    def test_ractor_map_repeated_pulls_after_completion_do_not_pull_upstream_again
      next_calls = 0
      sink =
        Sink.__send__(:new) do |stream|
          3.times.map { stream.next }
        end

      Source.each([1])
        .via(build_next_counting_flow { next_calls += 1 })
        .ractor_map(workers: 2, &IDENTITY_MAPPER)
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_ractor_map_copy_transfer_leaves_input_usable
      input = +"a"

      result =
        Source.each([input])
          .ractor_map(workers: 1, input_transfer: :copy, &MUTATING_MAPPER)
          .run_with(Sink.to_a)

      assert_equal ["a!"], result
      assert_equal "a", input
    end

    def test_ractor_map_move_transfer_moves_input
      input = +"a"

      result =
        Source.each([input])
          .ractor_map(workers: 1, input_transfer: :move, &MUTATING_MAPPER)
          .run_with(Sink.to_a)

      assert_equal ["a!"], result
      assert_raises(Ractor::MovedError) { input.bytesize }
    end

    def test_ractor_map_input_transfer_failure_becomes_ractor_map_error
      error = assert_raises(RactorMapError) do
        Source.each([Thread.current])
          .ractor_map(workers: 1, &IDENTITY_MAPPER)
          .run_with(Sink.to_a)
      end

      assert_equal 0, error.sequence
      assert_equal :input_transfer, error.kind
      assert_equal "TypeError", error.cause_class_name
    end

    def test_ractor_map_output_transfer_failure_becomes_ractor_map_error
      mapper = Ractor.shareable_proc { Thread.current }

      error = assert_raises(RactorMapError) do
        Source.each([1])
          .ractor_map(workers: 1, &mapper)
          .run_with(Sink.to_a)
      end

      assert_equal 0, error.sequence
      assert_equal :output_transfer, error.kind
      assert_equal "TypeError", error.cause_class_name
    end

    def test_ractor_map_normalizes_exception_subclass_mapper_failures
      mapper = Ractor.shareable_proc { raise Exception, "fatal map boom" } # rubocop:disable Lint/RaiseException

      error = assert_raises(RactorMapError) do
        Source.each([1])
          .ractor_map(workers: 1, &mapper)
          .run_with(Sink.to_a)
      end

      assert_equal 0, error.sequence
      assert_equal :worker, error.kind
      assert_equal "Exception", error.cause_class_name
      assert_equal "fatal map boom", error.cause_message
    end

    def test_ractor_map_detects_worker_termination_without_lifecycle_message
      with_ractor_map_worker_spawner(->(*) { Ractor.new { :stopped_without_messages } }) do
        error =
          Timeout.timeout(1) do
            assert_raises(RactorMapError) do
              Source.each([1])
                .ractor_map(workers: 1, &IDENTITY_MAPPER)
                .run_with(Sink.to_a)
            end
          end

        assert_equal 0, error.sequence
        assert_equal :worker_termination, error.kind
      end
    end

    def test_ractor_map_detects_active_worker_remote_error_without_lifecycle_message
      ready = ractor_map_envelope(:Ready)
      spawner =
        lambda do |worker_id, result_port, _transform, _output_transfer|
          Ractor.new(worker_id, result_port, ready) do |id, port, ready_message|
            Thread.current.report_on_exception = false
            port.send(ready_message.new(id))
            Ractor.receive
            raise "unhandled worker crash"
          end
        end

      with_ractor_map_worker_spawner(spawner) do
        error =
          Timeout.timeout(1) do
            assert_raises(RactorMapError) do
              Source.each([1])
                .ractor_map(workers: 1, &IDENTITY_MAPPER)
                .run_with(Sink.to_a)
            end
          end

        assert_equal 0, error.sequence
        assert_equal :worker_termination, error.kind
        assert_equal "Ractor::RemoteError", error.cause_class_name
      end
    end

    def test_ractor_map_attributes_remote_error_to_failed_worker_sequence
      ready = ractor_map_envelope(:Ready)
      worker_value = ractor_map_envelope(:WorkerValue)
      stopped = ractor_map_envelope(:Stopped)
      spawner =
        lambda do |worker_id, result_port, _transform, _output_transfer|
          Ractor.new(worker_id, result_port, ready, worker_value, stopped) do |id, port, ready_message, value_message,
                                                                               stopped_message|
            Thread.current.report_on_exception = false
            port.send(ready_message.new(id))

            message = Ractor.receive
            sequence = message.sequence
            value = message.value
            raise "sequence one crash" if sequence == 1

            sleep 0.05
            port.send(value_message.new(id, sequence, value))
            port.send(ready_message.new(id))
            Ractor.receive
            port.send(stopped_message.new(id))
          end
        end

      sink =
        Sink.__send__(:new) do |stream|
          stream.next
          stream.next
        end

      with_ractor_map_worker_spawner(spawner) do
        error =
          Timeout.timeout(1) do
            assert_raises(RactorMapError) do
              Source.each([1, 2])
                .ractor_map(workers: 2, &IDENTITY_MAPPER)
                .run_with(sink)
            end
          end

        assert_equal 1, error.sequence
        assert_equal :worker_termination, error.kind
        assert_equal "Ractor::RemoteError", error.cause_class_name
      end
    end

    def test_ractor_map_wait_does_not_block_async_reactor
      ticks = 0
      mapper =
        Ractor.shareable_proc do |value|
          sleep 0.05
          value
        end

      result =
        Sync do
          ticker =
            Async do
              3.times do
                sleep 0.01
                ticks += 1
              end
            end

          value =
            Source.each([1])
              .ractor_map(workers: 1, &mapper)
              .run_with(Sink.first)

          ticker.wait
          value
        end

      assert_equal 1, result
      assert_operator ticks, :>=, 3
    end

    def test_ractor_map_cleanup_wait_does_not_block_async_reactor
      ticks = 0
      mapper =
        Ractor.shareable_proc do |value|
          sleep 0.05 if value == 2
          value
        end

      result =
        Sync do
          ticker =
            Async do
              3.times do
                sleep 0.01
                ticks += 1
              end
            end

          value =
            Source.each([1, 2])
              .ractor_map(workers: 2, &mapper)
              .run_with(Sink.first)

          ticker.wait
          value
        end

      assert_equal 1, result
      assert_operator ticks, :>=, 3
    end

    def test_ractor_map_enqueue_waits_without_polling_when_queue_is_full
      boundary = ractor_map_boundary.new(nil, 1, :copy, :copy, IDENTITY_MAPPER)
      queue = Thread::SizedQueue.new(1)
      queue << :occupied
      errors = Thread::Queue.new

      boundary.define_singleton_method(:sleep) do |_interval|
        raise "enqueue polled instead of blocking"
      end

      thread =
        Thread.new do
          push_ractor_map_test_message(boundary, queue, :delivered)
        rescue StandardError => error
          errors << error
        end

      Timeout.timeout(1) do
        Thread.pass until thread.status == "sleep" || !thread.alive?
      end
      raise errors.pop unless errors.empty?

      assert_equal :occupied, queue.pop
      Timeout.timeout(1) { thread.join }
      raise errors.pop unless errors.empty?

      assert_equal :delivered, queue.pop
    end

    def test_ractor_map_close_wakes_coordinator_blocked_on_full_result_queue
      ready = ractor_map_envelope(:Ready)
      worker_value = ractor_map_envelope(:WorkerValue)
      stopped = ractor_map_envelope(:Stopped)
      spawner =
        lambda do |worker_id, result_port, _transform, _output_transfer|
          Ractor.new(worker_id, result_port, ready, worker_value, stopped) do |id, port, ready_message, value_message,
                                                                               stopped_message|
            port.send(ready_message.new(id))
            job = Ractor.receive

            port.send(value_message.new(id, job.sequence, job.value))
            3.times do |offset|
              port.send(value_message.new(id, job.sequence + offset + 1, job.value))
            end

            Ractor.receive
            port.send(stopped_message.new(id))
          end
        end
      sink =
        Sink.__send__(:new) do |stream|
          value = stream.next
          sleep 0.05
          value
        end

      with_ractor_map_worker_spawner(spawner) do
        result =
          Timeout.timeout(1) do
            Source.each([1])
              .ractor_map(workers: 1, &IDENTITY_MAPPER)
              .run_with(sink)
          end

        assert_equal 1, result
      end
    end

    private

    def ractor_map_boundary
      FiberStream.const_get(:Pull).__send__(:const_get, :RactorMapBoundary)
    end

    def ractor_map_envelope(name)
      ractor_map_boundary.const_get(name, false)
    end

    def push_ractor_map_test_message(boundary, queue, message)
      boundary.__send__(:push_until_delivered_or_closed, queue, message)
    end

    def explode_after_first(value)
      return value if value == 1

      raise "upstream boom"
    end

    def build_next_counting_flow(&on_next)
      Flow.__send__(:new) do |upstream|
        NextCountingStage.new(upstream, &on_next)
      end
    end

    def build_close_tracking_flow(&on_close)
      Flow.__send__(:new) do |upstream|
        CloseTrackingStage.new(upstream, &on_close)
      end
    end

    def build_close_raising_flow
      Flow.__send__(:new) do |upstream|
        CloseRaisingStage.new(upstream)
      end
    end

    def with_ractor_map_worker_spawner(spawner)
      boundary = FiberStream.const_get(:Pull).__send__(:const_get, :RactorMapBoundary)
      original = boundary.__send__(:method, :spawn_worker)

      redefine_ractor_map_worker_spawner(boundary, spawner)
      yield
    ensure
      redefine_ractor_map_worker_spawner(boundary, original) if boundary
    end

    def redefine_ractor_map_worker_spawner(boundary, callable)
      previous_verbose = $VERBOSE
      $VERBOSE = nil
      boundary.define_singleton_method(:spawn_worker, callable)
      boundary.__send__(:private_class_method, :spawn_worker)
    ensure
      $VERBOSE = previous_verbose
    end

    class NextCountingStage
      def initialize(upstream, &on_next)
        @upstream = upstream
        @on_next = on_next
      end

      def next
        @on_next.call
        @upstream.next
      end

      def close
        @upstream.close
      end
    end

    class CloseTrackingStage
      def initialize(upstream, &on_close)
        @upstream = upstream
        @on_close = on_close
        @closed = false
      end

      def next
        @upstream.next
      end

      def close
        return if @closed

        @closed = true
        @on_close.call
        @upstream.close
      end
    end

    class CloseRaisingStage
      def initialize(upstream)
        @upstream = upstream
        @closed = false
      end

      def next
        @upstream.next
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
        raise "close boom"
      end
    end
  end
end
