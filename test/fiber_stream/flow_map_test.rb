# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowMapTest < Minitest::Test
    include FlowTestHelpers

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

    def test_map_empty_source_emits_nothing
      result =
        Source.each([])
          .via(Flow.map { |value| value * 2 })
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_map_is_lazy
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.map { |value| value })

      refute pulled
    end

    def test_map_preserves_nil_transform_results
      result =
        Source.each([1, 2])
          .via(Flow.map { |value| value == 1 ? nil : value })
          .run_with(Sink.to_a)

      assert_equal [nil, 2], result
    end

    def test_map_each_demand_pulls_at_most_one_upstream_value
      next_calls = 0

      result =
        Source.each([1, 2])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.map { |value| value })
          .run_with(Sink.first)

      assert_equal 1, result
      assert_equal 1, next_calls
    end

    def test_map_propagates_close_after_early_sink_completion
      closed = false

      result =
        Source.each([1, 2, 3])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.map { |value| value })
          .run_with(Sink.first)

      assert_equal 1, result
      assert closed
    end

    def test_map_upstream_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(Flow.map { |value| value })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_map_transform_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.map { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    private

    def explode(_value)
      raise "boom"
    end
  end
end
