# frozen_string_literal: true

require "async"
require "async/scheduler"
require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowThrottleTest < Minitest::Test
    include FlowTestHelpers

    def test_throttle_preserves_ordered_values
      result =
        Sync do
          Source.each([1, 2, 3])
            .throttle(rate: 100, per: 1)
            .run_with(Sink.to_a)
        end

      assert_equal [1, 2, 3], result
    end

    def test_throttle_is_lazy_and_does_not_require_scheduler_until_demanded
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .throttle(rate: 1, per: 60)

      refute pulled
    end

    def test_throttle_allows_initial_burst_without_scheduler
      result =
        Source.each([1])
          .throttle(rate: 1, per: 60, burst: 1)
          .run_with(Sink.to_a)

      assert_equal [1], result
    end

    def test_throttle_requires_scheduler_when_default_limiter_must_wait
      error = assert_raises(SchedulerRequiredError) do
        Source.each([1, 2])
          .throttle(rate: 1, per: 60, burst: 1)
          .run_with(Sink.to_a)
      end

      assert_match(/Fiber.scheduler/, error.message)
    end

    def test_throttle_requires_non_blocking_fiber_when_default_limiter_must_wait
      scheduler = Async::Scheduler.new
      Fiber.set_scheduler(scheduler)

      error = assert_raises(SchedulerRequiredError) do
        Source.each([1, 2])
          .throttle(rate: 1, per: 60, burst: 1)
          .run_with(Sink.to_a)
      end

      assert_match(/non-blocking fiber/, error.message)
    ensure
      Fiber.set_scheduler(nil)
      scheduler&.close
    end

    def test_throttle_uses_fresh_default_limiter_per_materialization
      source = Source.each([1]).throttle(rate: 1, per: 60, burst: 1)

      assert_equal [1], source.run_with(Sink.to_a)
      assert_equal [1], source.run_with(Sink.to_a)
    end

    def test_source_throttle_paces_after_initial_burst
      elapsed =
        Sync do
          started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          Source.each([1, 2])
            .throttle(rate: 1, per: 0.02, burst: 1)
            .run_with(Sink.to_a)
          Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        end

      assert_operator elapsed, :>=, 0.015
    end

    def test_throttle_accepts_shared_custom_limiter
      limiter = CountingLimiter.new

      Source.each([1, 2]).throttle(limiter: limiter).run_with(Sink.to_a)

      assert_equal 2, limiter.calls
    end

    def test_throttle_rejects_missing_rate_and_limiter
      error = assert_raises(ArgumentError) { Flow.throttle }

      assert_match(/rate or limiter/, error.message)
    end

    def test_throttle_rejects_rate_with_limiter
      error = assert_raises(ArgumentError) do
        Flow.throttle(rate: 1, limiter: CountingLimiter.new)
      end

      assert_match(/rate and limiter/, error.message)
    end

    def test_throttle_rejects_per_with_limiter
      error = assert_raises(ArgumentError) do
        Flow.throttle(limiter: CountingLimiter.new, per: 1)
      end

      assert_match(/per.*limiter/, error.message)
    end

    def test_throttle_rejects_burst_with_limiter
      error = assert_raises(ArgumentError) do
        Flow.throttle(limiter: CountingLimiter.new, burst: 1)
      end

      assert_match(/burst.*limiter/, error.message)
    end

    def test_throttle_rejects_non_limiter_object
      error = assert_raises(TypeError) { Flow.throttle(limiter: Object.new) }

      assert_match(/limiter must respond to acquire/, error.message)
    end

    def test_throttle_rejects_false_burst
      assert_raises(TypeError) { Flow.throttle(rate: 1, burst: false) }
    end

    def test_throttle_rejects_unknown_keywords
      assert_raises(ArgumentError) { Flow.throttle(rate: 1, unknown: true) }
    end

    def test_empty_upstream_does_not_call_limiter
      limiter = CountingLimiter.new

      result =
        Source.each([])
          .throttle(limiter: limiter)
          .run_with(Sink.to_a)

      assert_equal [], result
      assert_equal 0, limiter.calls
    end

    def test_upstream_failure_before_value_does_not_call_limiter
      limiter = CountingLimiter.new

      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .throttle(limiter: limiter)
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
      assert_equal 0, limiter.calls
    end

    def test_limiter_failure_suppresses_pulled_value
      limiter = RaisingLimiter.new

      error = assert_raises(RuntimeError) do
        Source.each([1])
          .throttle(limiter: limiter)
          .run_with(Sink.to_a)
      end

      assert_equal "limiter boom", error.message
    end

    def test_custom_rate_limiter_block_failure_fails_stream
      limiter = RateLimiter.new(rate: 1) { raise "policy boom" }

      error = assert_raises(RuntimeError) do
        Source.each([1])
          .throttle(limiter: limiter)
          .run_with(Sink.to_a)
      end

      assert_equal "policy boom", error.message
    end

    def test_close_during_wait_suppresses_pulled_value
      limiter = ClosingLimiter.new
      stream = Pull.throttle(Pull.each([1]), limiter)
      limiter.stream = stream

      assert_equal Pull.const_get(:DONE), stream.next
      assert_equal 1, limiter.calls
    end

    def test_throttle_before_async_closes_upstream_after_early_completion
      closed = false

      result =
        Sync do
          Source.each([1, 2, 3])
            .via(build_close_tracking_flow { closed = true })
            .throttle(limiter: CountingLimiter.new)
            .async
            .run_with(Sink.first)
        end

      assert_equal 1, result
      assert closed
    end

    def test_throttle_before_buffer_closes_upstream_when_downstream_fails
      closed = false
      sink =
        Sink.build do |stream|
          stream.next
          raise "sink boom"
        end

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2, 3])
            .via(build_close_tracking_flow { closed = true })
            .throttle(limiter: CountingLimiter.new)
            .buffer(1)
            .run_with(sink)
        end
      end

      assert_equal "sink boom", error.message
      assert closed
    end

    def test_throttle_before_parallel_map_closes_upstream_after_early_completion
      closed = false

      result =
        Sync do
          Source.each([1, 2, 3])
            .via(build_close_tracking_flow { closed = true })
            .throttle(limiter: CountingLimiter.new)
            .parallel_map(concurrency: 2) { |value| value }
            .run_with(Sink.first)
        end

      assert_equal 1, result
      assert closed
    end

    def test_repeated_pulls_after_completion_do_not_call_limiter_again
      limiter = CountingLimiter.new
      sink = build_repeated_pull_sink(3)

      Source.each([1])
        .throttle(limiter: limiter)
        .run_with(sink)

      assert_equal 1, limiter.calls
    end

    class CountingLimiter
      attr_reader :calls

      def initialize
        @calls = 0
      end

      def acquire(permits: 1)
        @calls += permits
        nil
      end
    end

    class RaisingLimiter
      def acquire(**)
        raise "limiter boom"
      end
    end

    class ClosingLimiter
      attr_accessor :stream
      attr_reader :calls

      def initialize
        @stream = nil
        @calls = 0
      end

      def acquire(permits: 1)
        @calls += permits
        @stream.close
        nil
      end
    end
  end
end
