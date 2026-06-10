# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowDropTest < Minitest::Test
    include FlowTestHelpers

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

    def test_drop_empty_source_completes_without_emitting
      result =
        Source.each([])
          .via(Flow.drop(1))
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_drop_exact_count_emits_remaining_single_element
      result =
        Source.each([1, 2])
          .via(Flow.drop(1))
          .run_with(Sink.to_a)

      assert_equal [2], result
    end

    def test_drop_repeated_pulls_after_completion_do_not_pull_upstream_again
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Source.each([1, 2])
        .via(build_next_counting_flow { next_calls += 1 })
        .via(Flow.drop(1))
        .run_with(sink)

      assert_equal 3, next_calls
    end
  end
end
