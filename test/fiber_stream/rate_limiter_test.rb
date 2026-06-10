# frozen_string_literal: true

require "async"
require "async/scheduler"
require_relative "../test_helper"

module FiberStream
  class RateLimiterTest < Minitest::Test
    def test_acquire_grants_initial_burst_without_scheduler
      limiter = RateLimiter.new(rate: 1, per: 60, burst: 1)

      assert_nil limiter.acquire
    end

    def test_acquire_requires_scheduler_when_default_limiter_must_wait
      limiter = RateLimiter.new(rate: 1, per: 60, burst: 1)
      limiter.acquire

      error = assert_raises(SchedulerRequiredError) { limiter.acquire }

      assert_match(/Fiber.scheduler/, error.message)
    end

    def test_acquire_requires_non_blocking_fiber_when_default_limiter_must_wait
      scheduler = Async::Scheduler.new
      Fiber.set_scheduler(scheduler)
      limiter = RateLimiter.new(rate: 1, per: 60, burst: 1)
      limiter.acquire

      error = assert_raises(SchedulerRequiredError) { limiter.acquire }

      assert_match(/non-blocking fiber/, error.message)
    ensure
      Fiber.set_scheduler(nil)
      scheduler&.close
    end

    def test_default_limiter_paces_after_initial_burst
      limiter = RateLimiter.new(rate: 1, per: 0.02, burst: 1)

      elapsed =
        Sync do
          limiter.acquire
          started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          limiter.acquire
          Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        end

      assert_operator elapsed, :>=, 0.015
    end

    def test_shared_limiter_coordinates_concurrent_acquires
      limiter = RateLimiter.new(rate: 1, per: 0.02, burst: 1)

      elapsed =
        Sync do |task|
          started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          first = task.async { limiter.acquire }
          second = task.async { limiter.acquire }
          first.wait
          second.wait
          Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        end

      assert_operator elapsed, :>=, 0.015
    end

    def test_custom_block_wait_uses_scheduler_and_retries
      calls = 0
      limiter =
        RateLimiter.new(rate: 1, per: 1, burst: 1) do
          calls += 1
          calls == 1 ? 0.001 : nil
        end

      Sync { limiter.acquire }

      assert_equal 2, calls
    end

    def test_custom_block_positive_wait_requires_scheduler
      limiter = RateLimiter.new(rate: 1, per: 1, burst: 1) { 1 }

      error = assert_raises(SchedulerRequiredError) { limiter.acquire }

      assert_match(/Fiber.scheduler/, error.message)
    end

    def test_rejects_invalid_rate
      assert_raises(TypeError) { RateLimiter.new(rate: 1.5) }
      assert_raises(ArgumentError) { RateLimiter.new(rate: 0) }
    end

    def test_rejects_invalid_per
      assert_raises(TypeError) { RateLimiter.new(rate: 1, per: "1") }
      assert_raises(ArgumentError) { RateLimiter.new(rate: 1, per: 0) }
      assert_raises(ArgumentError) { RateLimiter.new(rate: 1, per: Float::INFINITY) }
      assert_raises(ArgumentError) { RateLimiter.new(rate: 1, per: Float::NAN) }
      assert_raises(ArgumentError) { RateLimiter.new(rate: 1, per: Complex(1, 0)) }
    end

    def test_rejects_invalid_burst
      assert_raises(TypeError) { RateLimiter.new(rate: 1, burst: false) }
      assert_raises(TypeError) { RateLimiter.new(rate: 1, burst: 1.5) }
      assert_raises(ArgumentError) { RateLimiter.new(rate: 1, burst: 0) }
    end

    def test_rejects_invalid_permits
      limiter = RateLimiter.new(rate: 1, burst: 1)

      assert_raises(TypeError) { limiter.acquire(permits: 1.5) }
      assert_raises(ArgumentError) { limiter.acquire(permits: 0) }
      assert_raises(ArgumentError) { limiter.acquire(permits: 2) }
    end

    def test_rejects_invalid_custom_wait_results
      assert_raises(TypeError) do
        RateLimiter.new(rate: 1) { "wait" }.acquire
      end

      assert_raises(ArgumentError) do
        RateLimiter.new(rate: 1) { Float::INFINITY }.acquire
      end

      assert_raises(ArgumentError) do
        RateLimiter.new(rate: 1) { Float::NAN }.acquire
      end

      assert_raises(ArgumentError) do
        RateLimiter.new(rate: 1) { Complex(1, 0) }.acquire
      end
    end

    def test_burst_defaults_to_rate
      limiter = RateLimiter.new(rate: 3, per: 60)

      3.times { assert_nil limiter.acquire }
      assert_raises(SchedulerRequiredError) { limiter.acquire }
    end

    def test_acquire_allows_permits_equal_to_burst_in_single_grant
      limiter = RateLimiter.new(rate: 2, per: 60, burst: 2)

      assert_nil limiter.acquire(permits: 2)
      assert_raises(SchedulerRequiredError) { limiter.acquire }
    end

    def test_acquire_rejects_permits_one_greater_than_burst
      limiter = RateLimiter.new(rate: 2, per: 60, burst: 2)

      assert_raises(ArgumentError) { limiter.acquire(permits: 3) }
    end

    def test_burst_larger_than_rate_grants_initial_capacity
      limiter = RateLimiter.new(rate: 1, per: 60, burst: 3)

      3.times { assert_nil limiter.acquire }
      assert_raises(SchedulerRequiredError) { limiter.acquire }
    end

    def test_custom_block_receives_request_with_permits_and_now
      request = nil
      limiter = RateLimiter.new(rate: 1, per: 1, burst: 1) do |req|
        request = req
        nil
      end

      limiter.acquire(permits: 1)

      assert_equal 1, request.permits
      assert_equal 1, request.rate
      assert_equal 1, request.burst
      assert_kind_of Float, request.now
    end
  end
end
