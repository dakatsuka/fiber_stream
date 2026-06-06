# frozen_string_literal: true

require "async"
require "async/scheduler"
require_relative "../test_helper"

module FiberStream
  class FlowParallelUnorderedMapTest < Minitest::Test
    def test_parallel_unordered_map_emits_completion_ordered_values
      result =
        Sync do
          Source.each([1, 2, 3])
            .parallel_unordered_map(concurrency: 3) do |value|
              sleep 0.03 if value == 1
              sleep 0.01 if value == 2
              value * 10
            end
            .run_with(Sink.to_a)
        end

      assert_equal [30, 20, 10], result
    end

    def test_parallel_unordered_map_convenience_delegates_to_flow
      result =
        Sync do
          Source.each([1, 2, 3])
            .parallel_unordered_map(concurrency: 2) { |value| value * 2 }
            .run_with(Sink.to_a)
        end

      assert_equal [2, 4, 6], result.sort
    end

    def test_parallel_unordered_map_is_lazy_and_does_not_require_scheduler_until_demanded
      pulled = false
      mapped = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.parallel_unordered_map(concurrency: 2) { mapped = true })

      refute pulled
      refute mapped
    end

    def test_parallel_unordered_map_requires_scheduler_when_demanded
      error = assert_raises(SchedulerRequiredError) do
        Source.each([1])
          .parallel_unordered_map(concurrency: 2) { |value| value }
          .run_with(Sink.to_a)
      end

      assert_match(/Fiber.scheduler/, error.message)
    end

    def test_parallel_unordered_map_requires_non_blocking_fiber_when_demanded
      scheduler = Async::Scheduler.new
      Fiber.set_scheduler(scheduler)

      error = assert_raises(SchedulerRequiredError) do
        Source.each([1])
          .parallel_unordered_map(concurrency: 2) { |value| value }
          .run_with(Sink.to_a)
      end

      assert_match(/non-blocking fiber/, error.message)
    ensure
      Fiber.set_scheduler(nil)
      scheduler&.close
    end

    def test_parallel_unordered_map_requires_block
      error = assert_raises(ArgumentError) do
        Flow.parallel_unordered_map(concurrency: 2)
      end

      assert_match(/missing block/, error.message)
    end

    def test_parallel_unordered_map_rejects_non_integer_concurrency
      error = assert_raises(TypeError) do
        Flow.parallel_unordered_map(concurrency: 1.5) { |value| value }
      end

      assert_match(/concurrency must be an Integer/, error.message)
    end

    def test_parallel_unordered_map_rejects_zero_concurrency
      error = assert_raises(ArgumentError) do
        Source.each([1]).parallel_unordered_map(concurrency: 0) { |value| value }
      end

      assert_match(/concurrency must be positive/, error.message)
    end

    def test_parallel_unordered_map_limits_active_mapping_blocks
      active = 0
      max_active = 0

      Sync do
        Source.each(1.upto(8))
          .parallel_unordered_map(concurrency: 2) do |value|
            active += 1
            max_active = [max_active, active].max
            sleep 0.01
            value
          ensure
            active -= 1
          end
          .run_with(Sink.to_a)
      end

      assert_equal 2, max_active
    end

    def test_parallel_unordered_map_bounds_pulled_but_unemitted_values
      pulled = 0

      Sync do
        Source.each(1.upto(100))
          .via(build_next_counting_flow { pulled += 1 })
          .parallel_unordered_map(concurrency: 2) do |value|
            sleep 0.02
            value
          end
          .run_with(Sink.first)

        sleep 0
      end

      assert_operator pulled, :<=, 3
    end

    def test_parallel_unordered_map_fails_fast_on_mapping_error
      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2])
            .parallel_unordered_map(concurrency: 2) do |value|
              if value == 1
                sleep 0.03
                value
              else
                raise "map boom"
              end
            end
            .run_with(Sink.to_a)
        end
      end

      assert_equal "map boom", error.message
    end

    def test_parallel_unordered_map_suppresses_in_flight_mapping_error_after_early_completion
      result =
        Sync do
          Source.each([1, 2])
            .parallel_unordered_map(concurrency: 2) do |value|
              if value == 2
                sleep 0.02
                raise "ignored boom"
              end

              value
            end
            .run_with(Sink.first)
        end

      assert_equal 1, result
    end

    def test_parallel_unordered_map_closes_upstream_after_early_completion
      closed = false

      result =
        Sync do
          Source.each([1, 2, 3])
            .via(build_close_tracking_flow { closed = true })
            .parallel_unordered_map(concurrency: 2) { |value| value }
            .run_with(Sink.first)
        end

      assert_equal 1, result
      assert closed
    end

    def test_parallel_unordered_map_requests_in_flight_worker_cancellation_after_early_completion
      started_second = false
      completed_after_sleep = false

      Sync do
        Source.each([1, 2])
          .parallel_unordered_map(concurrency: 2) do |value|
            if value == 2
              started_second = true
              sleep 0.05
              completed_after_sleep = true
            end

            value
          end
          .run_with(Sink.first)

        sleep 0.1
      end

      assert started_second
      refute completed_after_sleep
    end

    def test_parallel_unordered_map_preserves_close_error_after_early_completion
      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2, 3])
            .via(build_close_raising_flow)
            .parallel_unordered_map(concurrency: 2) { |value| value }
            .run_with(Sink.first)
        end
      end

      assert_equal "close boom", error.message
    end

    def test_parallel_unordered_map_preserves_in_progress_producer_close_error_after_early_completion
      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1])
            .via(build_slow_close_raising_flow)
            .parallel_unordered_map(concurrency: 2) do |value|
              sleep 0.01
              value
            end
            .run_with(Sink.first)
        end
      end

      assert_equal "close boom", error.message
    end

    def test_parallel_unordered_map_waits_for_in_flight_work_before_normal_completion
      result =
        Sync do
          Source.each([1, 2])
            .parallel_unordered_map(concurrency: 2) do |value|
              sleep 0.03 if value == 1
              value * 10
            end
            .run_with(Sink.to_a)
        end

      assert_equal [20, 10], result
    end

    def test_parallel_unordered_map_mapping_failure_wins_over_delayed_producer_close_error
      sink =
        Sink.__send__(:new) do |stream|
          [stream.next, stream.next]
        end

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2])
            .via(build_close_raising_flow)
            .parallel_unordered_map(concurrency: 2) do |value|
              if value == 1
                sleep 0.03
                raise "map boom"
              end

              value
            end
            .run_with(sink)
        end
      end

      assert_equal "map boom", error.message
    end

    def test_parallel_unordered_map_propagates_producer_close_error_after_normal_completion
      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1])
            .via(build_close_raising_flow)
            .parallel_unordered_map(concurrency: 2) { |value| value }
            .run_with(Sink.to_a)
        end
      end

      assert_equal "close boom", error.message
    end

    def test_parallel_unordered_map_prefers_upstream_error_over_close_error
      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2])
            .via(Flow.map { |value| explode_after_first(value) })
            .via(build_close_raising_flow)
            .parallel_unordered_map(concurrency: 2) { |value| value }
            .run_with(Sink.to_a)
        end
      end

      assert_equal "upstream boom", error.message
    end

    def test_parallel_unordered_map_repeated_pulls_after_completion_do_not_pull_upstream_again
      next_calls = 0
      sink =
        Sink.__send__(:new) do |stream|
          3.times.map { stream.next }
        end

      Sync do
        Source.each([1])
          .via(build_next_counting_flow { next_calls += 1 })
          .parallel_unordered_map(concurrency: 2) { |value| value }
          .run_with(sink)
      end

      assert_equal 2, next_calls
    end

    private

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

    def build_slow_close_raising_flow
      Flow.__send__(:new) do |upstream|
        SlowCloseRaisingStage.new(upstream)
      end
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

    class SlowCloseRaisingStage
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
        sleep 0.05
        raise "close boom"
      end
    end
  end
end
