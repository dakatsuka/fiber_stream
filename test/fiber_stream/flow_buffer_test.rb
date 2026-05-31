# frozen_string_literal: true

require "async"
require_relative "../test_helper"

module FiberStream
  class FlowBufferTest < Minitest::Test
    def test_buffer_preserves_ordered_values
      result =
        Sync do
          Source.each([1, 2, 3])
            .buffer(2)
            .run_with(Sink.to_a)
        end

      assert_equal [1, 2, 3], result
    end

    def test_buffer_is_lazy_and_does_not_require_scheduler_until_demanded
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.buffer(1))

      refute pulled
    end

    def test_buffer_requires_scheduler_when_demanded
      error = assert_raises(SchedulerRequiredError) do
        Source.each([1])
          .buffer(1)
          .run_with(Sink.to_a)
      end

      assert_match(/Fiber.scheduler/, error.message)
    end

    def test_buffer_rejects_non_integer_count
      error = assert_raises(TypeError) do
        Flow.buffer(1.5)
      end

      assert_match(/count must be an Integer/, error.message)
    end

    def test_buffer_rejects_zero_count
      error = assert_raises(ArgumentError) do
        Source.each([1]).buffer(0)
      end

      assert_match(/count must be positive/, error.message)
    end

    def test_buffer_bounds_prefetch
      pulled = 0
      values = 1.upto(100).to_a

      Sync do
        Source.each(values)
          .via(build_next_counting_flow { pulled += 1 })
          .buffer(2)
          .run_with(Sink.first)

        sleep 0
      end

      assert_operator pulled, :<=, 4
    end

    def test_buffer_wakes_full_buffer_producer_and_preserves_close_error
      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2, 3, 4])
            .via(build_close_raising_flow)
            .buffer(1)
            .run_with(Sink.first)
        end
      end

      assert_equal "close boom", error.message
    end

    def test_buffer_propagates_upstream_errors_after_buffered_values
      sink =
        Sink.__send__(:new) do |stream|
          [stream.next, stream.next]
        end

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2])
            .via(Flow.map { |value| explode_after_first(value) })
            .buffer(1)
            .run_with(sink)
        end
      end

      assert_equal "upstream boom", error.message
    end

    def test_buffer_suppresses_queued_upstream_error_after_early_completion
      result =
        Sync do
          Source.each([1, 2])
            .via(Flow.map { |value| raise "queued boom" if value == 2; value })
            .buffer(2)
            .run_with(Sink.first)
        end

      assert_equal 1, result
    end

    def test_buffer_suppresses_queued_upstream_and_close_errors_after_early_completion
      result =
        Sync do
          Source.each([1, 2])
            .via(Flow.map { |value| explode_after_first(value) })
            .via(build_close_raising_flow)
            .buffer(2)
            .run_with(Sink.first)
        end

      assert_equal 1, result
    end

    def test_buffer_closes_upstream_after_early_completion
      closed = false

      result =
        Sync do
          Source.each([1, 2, 3])
            .via(build_close_tracking_flow { closed = true })
            .buffer(1)
            .run_with(Sink.first)
        end

      assert_equal 1, result
      assert closed
    end

    def test_buffer_closes_upstream_when_downstream_fails
      closed = false
      sink =
        Sink.__send__(:new) do |stream|
          stream.next
          raise "sink boom"
        end

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2, 3])
            .via(build_close_tracking_flow { closed = true })
            .buffer(1)
            .run_with(sink)
        end
      end

      assert_equal "sink boom", error.message
      assert closed
    end

    def test_buffer_propagates_producer_close_error_after_normal_completion
      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1])
            .via(build_close_raising_flow)
            .buffer(1)
            .run_with(Sink.to_a)
        end
      end

      assert_equal "close boom", error.message
    end

    def test_buffer_prefers_upstream_error_over_producer_close_error
      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2])
            .via(Flow.map { |value| explode_after_first(value) })
            .via(build_close_raising_flow)
            .buffer(1)
            .run_with(Sink.to_a)
        end
      end

      assert_equal "upstream boom", error.message
    end

    def test_buffer_repeated_pulls_after_completion_do_not_pull_upstream_again
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Sync do
        Source.each([1])
          .via(build_next_counting_flow { next_calls += 1 })
          .buffer(1)
          .run_with(sink)
      end

      assert_equal 2, next_calls
    end

    private

    def explode_after_first(value)
      return value if value == 1

      raise "upstream boom"
    end

    def build_close_tracking_flow(&on_close)
      Flow.__send__(:new) do |upstream|
        CloseTrackingStage.new(upstream, &on_close)
      end
    end

    def build_next_counting_flow(&on_next)
      Flow.__send__(:new) do |upstream|
        NextCountingStage.new(upstream, &on_next)
      end
    end

    def build_close_raising_flow
      Flow.__send__(:new) do |upstream|
        CloseRaisingStage.new(upstream)
      end
    end

    def build_repeated_pull_sink(count)
      Sink.__send__(:new) do |stream|
        count.times.map { stream.next }
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
