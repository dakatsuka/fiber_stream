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

    def test_take_limits_elements
      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.take(2))
          .run_with(Sink.to_a)

      assert_equal [1, 2], result
    end

    def test_take_zero_completes_without_pulling_and_closes_on_first_demand
      next_calls = 0
      closed = false

      result =
        Source.each([1])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.take(0))
          .run_with(Sink.first)

      assert_nil result
      assert_equal 0, next_calls
      assert closed
    end

    def test_take_closes_upstream_while_forwarding_limit_element
      closed = false
      sink =
        Sink.__send__(:new) do |stream|
          first = stream.next
          [first, closed]
        end

      result =
        Source.each([1, 2])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.take(1))
          .run_with(sink)

      assert_equal [1, true], result
    end

    def test_take_does_not_pull_upstream_after_limit
      next_calls = 0
      flow = build_next_counting_flow { next_calls += 1 }
      sink = build_repeated_pull_sink(3)

      Source.each([1, 2, 3])
        .via(flow)
        .via(Flow.take(1))
        .run_with(sink)

      assert_equal 1, next_calls
    end

    def test_take_is_lazy
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.take(1))

      refute pulled
    end

    def test_take_rejects_negative_count
      error = assert_raises(ArgumentError) do
        Flow.take(-1)
      end

      assert_match(/count must be non-negative/, error.message)
    end

    def test_take_rejects_non_integer_count
      error = assert_raises(TypeError) do
        Flow.take(1.5)
      end

      assert_match(/count must be an Integer/, error.message)
    end

    def test_drop_skips_prefix
      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.drop(2))
          .run_with(Sink.to_a)

      assert_equal [3, 4], result
    end

    def test_drop_zero_passes_through
      result =
        Source.each([1, 2])
          .via(Flow.drop(0))
          .run_with(Sink.to_a)

      assert_equal [1, 2], result
    end

    def test_drop_count_greater_than_stream_length_completes
      result =
        Source.each([1, 2])
          .via(Flow.drop(3))
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_drop_first_demand_pulls_dropped_prefix_and_first_retained_value
      next_calls = 0

      result =
        Source.each([1, 2, 3, 4])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.drop(2))
          .run_with(Sink.first)

      assert_equal 3, result
      assert_equal 3, next_calls
    end

    def test_drop_after_prefix_pulls_one_upstream_value_per_demand
      next_calls = 0
      sink = build_repeated_pull_sink(2)

      result =
        Source.each([1, 2, 3, 4])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.drop(2))
          .run_with(sink)

      assert_equal [3, 4], result
      assert_equal 4, next_calls
    end

    def test_drop_does_not_pull_upstream_again_after_completion
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Source.each([1])
        .via(build_next_counting_flow { next_calls += 1 })
        .via(Flow.drop(1))
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_drop_exact_count_does_not_pull_upstream_again_after_completion
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Source.each([1, 2])
        .via(build_next_counting_flow { next_calls += 1 })
        .via(Flow.drop(2))
        .run_with(sink)

      assert_equal 3, next_calls
    end

    def test_drop_is_lazy
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.drop(1))

      refute pulled
    end

    def test_drop_propagates_close_after_early_sink_completion
      closed = false

      result =
        Source.each([1, 2, 3])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.drop(1))
          .run_with(Sink.first)

      assert_equal 2, result
      assert closed
    end

    def test_drop_prefix_upstream_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(Flow.drop(1))
          .run_with(Sink.first)
      end

      assert_equal "next boom", error.message
    end

    def test_drop_retained_upstream_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_next_raising_flow(raise_on_call: 2))
          .via(Flow.drop(1))
          .run_with(Sink.first)
      end

      assert_equal "next boom", error.message
    end

    def test_drop_rejects_negative_count
      error = assert_raises(ArgumentError) do
        Flow.drop(-1)
      end

      assert_match(/count must be non-negative/, error.message)
    end

    def test_drop_rejects_non_integer_count
      error = assert_raises(TypeError) do
        Flow.drop(1.5)
      end

      assert_match(/count must be an Integer/, error.message)
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

    def build_next_raising_flow(raise_on_call:)
      Flow.__send__(:new) do |upstream|
        NextRaisingStage.new(upstream, raise_on_call)
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

    class NextRaisingStage
      def initialize(upstream, raise_on_call)
        @upstream = upstream
        @raise_on_call = raise_on_call
        @calls = 0
      end

      def next
        @calls += 1
        raise "next boom" if @calls == @raise_on_call

        @upstream.next
      end

      def close
        @upstream.close
      end
    end
  end
end
