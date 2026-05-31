# frozen_string_literal: true

require "async"
require_relative "../test_helper"

module FiberStream
  class FlowAsyncTest < Minitest::Test
    def test_async_is_lazy_and_does_not_require_scheduler_until_demanded
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.async)

      refute pulled
    end

    def test_async_requires_scheduler_when_demanded
      error = assert_raises(SchedulerRequiredError) do
        Source.each([1])
          .via(Flow.async)
          .run_with(Sink.to_a)
      end

      assert_match(/Fiber.scheduler/, error.message)
    end

    def test_async_preserves_ordered_values
      result =
        Sync do
          Source.each([1, 2, 3])
            .via(Flow.map { |value| value * 2 })
            .via(Flow.async)
            .run_with(Sink.to_a)
        end

      assert_equal [2, 4, 6], result
    end

    def test_async_convenience_delegates_to_flow_async
      result =
        Sync do
          Source.each([1, 2, 3])
            .async
            .run_with(Sink.to_a)
        end

      assert_equal [1, 2, 3], result
    end

    def test_async_propagates_upstream_errors
      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1])
            .via(Flow.map { |value| explode(value) })
            .via(Flow.async)
            .run_with(Sink.to_a)
        end
      end

      assert_equal "async boom", error.message
    end

    def test_async_closes_upstream_after_early_sink_completion
      closed = false

      result =
        Sync do
          Source.each([1, 2, 3])
            .via(build_close_tracking_flow { closed = true })
            .via(Flow.async)
            .run_with(Sink.first)
        end

      assert_equal 1, result
      assert closed
    end

    def test_async_closes_upstream_when_downstream_fails
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
            .via(Flow.async)
            .run_with(sink)
        end
      end

      assert_equal "sink boom", error.message
      assert closed
    end

    def test_async_preserves_upstream_close_error_after_early_completion
      stage = nil
      flow =
        Flow.__send__(:new) do |upstream|
          stage = CloseRaisingOnceStage.new(upstream)
        end

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each([1, 2, 3])
            .via(flow)
            .via(Flow.async)
            .run_with(Sink.first)
        end
      end

      assert_equal "close boom", error.message
      assert_equal 1, stage.close_calls
    end

    def test_async_does_not_pull_past_early_downstream_completion
      pulled = 0

      Sync do
        Source.each([1, 2, 3, 4])
          .via(build_next_counting_flow { pulled += 1 })
          .via(Flow.async)
          .run_with(Sink.first)

        sleep 0
      end

      assert_operator pulled, :<=, 2
    end

    def test_async_does_not_pull_upstream_again_after_completion
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Sync do
        Source.each([1])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.async)
          .run_with(sink)
      end

      assert_equal 2, next_calls
    end

    private

    def explode(_value)
      raise "async boom"
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

    class CloseRaisingOnceStage
      attr_reader :close_calls

      def initialize(upstream)
        @upstream = upstream
        @close_calls = 0
      end

      def next
        @upstream.next
      end

      def close
        @close_calls += 1
        @upstream.close
        raise "close boom" if @close_calls == 1
      end
    end
  end
end
