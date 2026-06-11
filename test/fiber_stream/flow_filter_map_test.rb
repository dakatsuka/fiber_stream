# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowFilterMapTest < Minitest::Test
    include FlowTestHelpers

    def test_filter_map_emits_truthy_transformed_values
      result =
        Source.each(["1", "x", "2"])
          .via(Flow.filter_map { |text| Integer(text, exception: false) })
          .run_with(Sink.to_a)

      assert_equal [1, 2], result
    end

    def test_filter_map_drops_false_and_nil_transform_results
      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.filter_map do |value|
            next false if value == 1
            next nil if value == 2

            value * 10
          end)
          .run_with(Sink.to_a)

      assert_equal [30, 40], result
    end

    def test_filter_map_never_emits_false_or_nil_results
      result =
        Source.each([true, false, nil])
          .via(Flow.filter_map { |value| value })
          .run_with(Sink.to_a)

      assert_equal [true], result
    end

    def test_filter_map_preserves_accepted_output_order
      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.filter_map { |value| value.even? ? "even:#{value}" : nil })
          .run_with(Sink.to_a)

      assert_equal ["even:2", "even:4"], result
    end

    def test_filter_map_pulls_until_truthy_result_for_each_downstream_demand
      transform_calls = 0
      next_calls = 0

      result =
        Source.each([1, 2, 3, 4])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.filter_map do |value|
            transform_calls += 1
            value.even? ? value : nil
          end)
          .run_with(Sink.first)

      assert_equal 2, result
      assert_equal 2, transform_calls
      assert_equal 2, next_calls
    end

    def test_filter_map_pulls_entire_upstream_when_nothing_is_accepted
      next_calls = 0
      sink = build_repeated_pull_sink(2)

      result =
        Source.each([1, 2])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.filter_map { false })
          .run_with(sink)

      assert_equal 2, result.size
      assert(result.all? { |value| Pull.done?(value) })
      assert_equal 3, next_calls
    end

    def test_filter_map_does_not_pull_upstream_again_after_completion
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Source.each([1])
        .via(build_next_counting_flow { next_calls += 1 })
        .via(Flow.filter_map { nil })
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_filter_map_empty_source_emits_nothing
      result =
        Source.each([])
          .via(Flow.filter_map { |value| value })
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_filter_map_is_lazy
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.filter_map { |value| value })

      refute pulled
    end

    def test_filter_map_propagates_close_after_normal_completion
      closed = false

      result =
        Source.each([1])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.filter_map { |value| value })
          .run_with(Sink.to_a)

      assert_equal [1], result
      assert closed
    end

    def test_filter_map_propagates_close_after_early_sink_completion
      closed = false

      result =
        Source.each([1, 2, 3])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.filter_map { |value| value })
          .run_with(Sink.first)

      assert_equal 1, result
      assert closed
    end

    def test_filter_map_cleanup_close_failure_after_success_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.filter_map { |value| value })
          .run_with(Sink.to_a)
      end

      assert_equal "close boom", error.message
    end

    def test_filter_map_cleanup_close_failure_after_early_sink_completion_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_close_raising_flow)
          .via(Flow.filter_map { |value| value })
          .run_with(Sink.first)
      end

      assert_equal "close boom", error.message
    end

    def test_filter_map_transform_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(Flow.filter_map { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_filter_map_transform_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.filter_map { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_filter_map_upstream_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(Flow.filter_map { |value| value })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_filter_map_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_raising_flow)
          .via(Flow.filter_map { |value| value })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_filter_map_requires_block
      error = assert_raises(ArgumentError) do
        Flow.filter_map
      end

      assert_match(/missing block/, error.message)
    end

    private

    def explode(_value)
      raise "boom"
    end
  end
end
