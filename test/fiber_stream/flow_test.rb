# frozen_string_literal: true

require_relative "../test_helper"

module FiberStream
  class FlowTest < Minitest::Test
    def test_map_transforms_elements
      result =
        Source.each([1, 2, 3])
          .via(Flow.map { |value| value * 2 })
          .run_with(Sink.to_a)

      assert_equal [2, 4, 6], result
    end

    def test_map_flows_compose_in_order
      result =
        Source.each([1, 2, 3])
          .via(Flow.map { |value| value * 2 })
          .via(Flow.map(&:to_s))
          .run_with(Sink.to_a)

      assert_equal ["2", "4", "6"], result
    end

    def test_map_exception_fails_stream
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(Flow.map { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_map_requires_block
      error = assert_raises(ArgumentError) do
        Flow.map
      end

      assert_match(/missing block/, error.message)
    end

    def test_map_does_not_pull_upstream_again_after_completion
      next_calls = 0
      flow = build_next_counting_flow { next_calls += 1 }
      sink = build_repeated_pull_sink(3)

      Source.each([1])
        .via(flow)
        .via(Flow.map { |value| value })
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_map_propagates_close_after_normal_completion
      closed = false
      flow = build_close_tracking_flow { closed = true }

      result =
        Source.each([1])
          .via(flow)
          .via(Flow.map { |value| value })
          .run_with(Sink.to_a)

      assert_equal [1], result
      assert closed
    end

    def test_select_keeps_matching_elements
      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.select(&:even?))
          .run_with(Sink.to_a)

      assert_equal [2, 4], result
    end

    def test_select_drops_false_and_nil_predicate_results
      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.select do |value|
            next false if value == 1
            next nil if value == 2

            true
          end)
          .run_with(Sink.to_a)

      assert_equal [3, 4], result
    end

    def test_select_pulls_until_match_for_each_downstream_demand
      predicate_calls = 0

      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.select do |value|
            predicate_calls += 1
            value.even?
          end)
          .run_with(Sink.first)

      assert_equal 2, result
      assert_equal 2, predicate_calls
    end

    def test_select_does_not_pull_upstream_again_after_completion
      next_calls = 0
      flow = build_next_counting_flow { next_calls += 1 }
      sink = build_repeated_pull_sink(2)

      Source.each([1])
        .via(flow)
        .via(Flow.select { false })
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_select_propagates_close_after_normal_completion
      closed = false
      flow = build_close_tracking_flow { closed = true }

      result =
        Source.each([1])
          .via(flow)
          .via(Flow.select { true })
          .run_with(Sink.to_a)

      assert_equal [1], result
      assert closed
    end

    def test_select_propagates_close_after_early_sink_completion
      closed = false
      flow = build_close_tracking_flow { closed = true }

      result =
        Source.each([1, 2, 3])
          .via(flow)
          .via(Flow.select { true })
          .run_with(Sink.first)

      assert_equal 1, result
      assert closed
    end

    def test_select_exception_fails_stream
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(Flow.select { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_select_requires_block
      error = assert_raises(ArgumentError) do
        Flow.select
      end

      assert_match(/missing block/, error.message)
    end

    private

    def explode(_value)
      raise "boom"
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
  end
end
