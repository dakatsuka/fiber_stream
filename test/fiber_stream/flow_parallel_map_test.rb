# frozen_string_literal: true

require "async"
require "async/scheduler"
require_relative "../test_helper"

module FiberStream
  class FlowParallelMapTest < Minitest::Test
    def test_parallel_map_preserves_ordered_values
      result =
        Sync do
          Source.each([1, 2, 3])
            .parallel_map(concurrency: 2) do |value|
              sleep 0.02 if value == 1
              value * 10
            end
            .run_with(Sink.to_a)
        end

      assert_equal [10, 20, 30], result
    end

    def test_parallel_map_is_lazy_and_does_not_require_scheduler_until_demanded
      pulled = false
      mapped = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.parallel_map(concurrency: 2) { mapped = true })

      refute pulled
      refute mapped
    end

    def test_parallel_map_requires_scheduler_when_demanded
      error = assert_raises(SchedulerRequiredError) do
        Source.each([1])
          .parallel_map(concurrency: 2) { |value| value }
          .run_with(Sink.to_a)
      end

      assert_match(/Fiber.scheduler/, error.message)
    end

    def test_parallel_map_requires_non_blocking_fiber_when_demanded
      scheduler = Async::Scheduler.new
      Fiber.set_scheduler(scheduler)

      error = assert_raises(SchedulerRequiredError) do
        Source.each([1])
          .parallel_map(concurrency: 2) { |value| value }
          .run_with(Sink.to_a)
      end

      assert_match(/non-blocking fiber/, error.message)
    ensure
      Fiber.set_scheduler(nil)
      scheduler&.close
    end

    def test_parallel_map_requires_block
      error = assert_raises(ArgumentError) do
        Flow.parallel_map(concurrency: 2)
      end

      assert_match(/missing block/, error.message)
    end

    def test_parallel_map_rejects_non_integer_concurrency
      error = assert_raises(TypeError) do
        Flow.parallel_map(concurrency: 1.5) { |value| value }
      end

      assert_match(/concurrency must be an Integer/, error.message)
    end

    def test_parallel_map_rejects_zero_concurrency
      error = assert_raises(ArgumentError) do
        Source.each([1]).parallel_map(concurrency: 0) { |value| value }
      end

      assert_match(/concurrency must be positive/, error.message)
    end

    def test_parallel_map_limits_active_mapping_blocks
      active = 0
      max_active = 0

      Sync do
        Source.each(1.upto(8))
          .parallel_map(concurrency: 2) do |value|
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

    def test_parallel_map_bounds_pulled_but_unemitted_values
      pulled = 0

      Sync do
        Source.each(1.upto(100))
          .via(build_next_counting_flow { pulled += 1 })
          .parallel_map(concurrency: 2) do |value|
            sleep 0.02
            value
          end
          .run_with(Sink.first)

        sleep 0
      end

      assert_operator pulled, :<=, 3
    end

    def test_parallel_map_delivers_later_failure_after_earlier_value
      observed = []
      sink =
        Sink.build do |stream|
          observed << stream.next
          stream.next
        end

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2])
            .parallel_map(concurrency: 2) do |value|
              raise "map boom" if value == 2

              sleep 0.02
              value
            end
            .run_with(sink)
        end
      end

      assert_equal "map boom", error.message
      assert_equal [1], observed
    end

    def test_parallel_map_delivers_lowest_sequence_failure
      observed = []
      sink =
        Sink.build do |stream|
          observed << stream.next
          stream.next
        end

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2, 3])
            .parallel_map(concurrency: 3) do |value|
              raise "third boom" if value == 3

              if value == 2
                sleep 0.02
                raise "second boom"
              end

              sleep 0.01
              value
            end
            .run_with(sink)
        end
      end

      assert_equal "second boom", error.message
      assert_equal [1], observed
    end

    def test_parallel_map_propagates_upstream_errors_after_earlier_values
      sink =
        Sink.build do |stream|
          [stream.next, stream.next]
        end

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2])
            .via(Flow.map { |value| explode_after_first(value) })
            .parallel_map(concurrency: 2) { |value| value }
            .run_with(sink)
        end
      end

      assert_equal "upstream boom", error.message
    end

    def test_parallel_map_suppresses_in_flight_mapping_error_after_early_completion
      result =
        Sync do
          Source.each([1, 2])
            .parallel_map(concurrency: 2) do |value|
              raise "ignored boom" if value == 2

              value
            end
            .run_with(Sink.first)
        end

      assert_equal 1, result
    end

    def test_parallel_map_closes_upstream_after_early_completion
      closed = false

      result =
        Sync do
          Source.each([1, 2, 3])
            .via(build_close_tracking_flow { closed = true })
            .parallel_map(concurrency: 2) { |value| value }
            .run_with(Sink.first)
        end

      assert_equal 1, result
      assert closed
    end

    def test_parallel_map_requests_in_flight_worker_cancellation_after_early_completion
      started_second = false
      completed_after_sleep = false

      Sync do
        Source.each([1, 2])
          .parallel_map(concurrency: 2) do |value|
            if value == 2
              started_second = true
              sleep 0.05
              completed_after_sleep = true
            else
              sleep 0.01
            end

            value
          end
          .run_with(Sink.first)

        sleep 0.1
      end

      assert started_second
      refute completed_after_sleep
    end

    def test_parallel_map_preserves_close_error_after_early_completion
      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2, 3])
            .via(build_close_raising_flow)
            .parallel_map(concurrency: 2) { |value| value }
            .run_with(Sink.first)
        end
      end

      assert_equal "close boom", error.message
    end

    def test_parallel_map_propagates_producer_close_error_after_normal_completion
      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1])
            .via(build_close_raising_flow)
            .parallel_map(concurrency: 2) { |value| value }
            .run_with(Sink.to_a)
        end
      end

      assert_equal "close boom", error.message
    end

    def test_parallel_map_prefers_upstream_error_over_close_error
      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2])
            .via(Flow.map { |value| explode_after_first(value) })
            .via(build_close_raising_flow)
            .parallel_map(concurrency: 2) { |value| value }
            .run_with(Sink.to_a)
        end
      end

      assert_equal "upstream boom", error.message
    end

    def test_parallel_map_repeated_pulls_after_completion_do_not_pull_upstream_again
      next_calls = 0
      sink =
        Sink.build do |stream|
          3.times.map { stream.next }
        end

      Sync do
        Source.each([1])
          .via(build_next_counting_flow { next_calls += 1 })
          .parallel_map(concurrency: 2) { |value| value }
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
      Flow.build do |upstream|
        NextCountingStage.new(upstream, &on_next)
      end
    end

    def build_close_tracking_flow(&on_close)
      Flow.build do |upstream|
        CloseTrackingStage.new(upstream, &on_close)
      end
    end

    def build_close_raising_flow
      Flow.build do |upstream|
        CloseRaisingStage.new(upstream)
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
  end
end
