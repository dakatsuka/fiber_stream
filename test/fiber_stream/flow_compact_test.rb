# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowCompactTest < Minitest::Test
    include FlowTestHelpers

    def test_compact_drops_nil_and_retains_false
      result =
        Source.each([1, nil, false, 2])
          .via(Flow.compact)
          .run_with(Sink.to_a)

      assert_equal [1, false, 2], result
    end

    def test_compact_preserves_retained_output_order
      result =
        Source.each([3, nil, 1, false, nil, 2])
          .via(Flow.compact)
          .run_with(Sink.to_a)

      assert_equal [3, 1, false, 2], result
    end

    def test_compact_preserves_retained_object_identity
      first = Object.new
      second = Object.new

      result =
        Source.each([first, nil, second])
          .via(Flow.compact)
          .run_with(Sink.to_a)

      assert_equal 2, result.size
      assert_same first, result[0]
      assert_same second, result[1]
    end

    def test_compact_pulls_until_non_nil_for_each_downstream_demand
      next_calls = 0

      result =
        Source.each([nil, nil, "value"])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.compact)
          .run_with(Sink.first)

      assert_equal "value", result
      assert_equal 3, next_calls
    end

    def test_compact_does_not_pull_upstream_again_after_completion
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Source.each([nil])
        .via(build_next_counting_flow { next_calls += 1 })
        .via(Flow.compact)
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_compact_empty_source_emits_nothing
      result =
        Source.each([])
          .via(Flow.compact)
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_compact_all_nil_elements_complete_without_emitting
      result =
        Source.each([nil, nil])
          .via(Flow.compact)
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_compact_is_lazy
      pulled = false

      Source.each([nil])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.compact)

      refute pulled
    end

    def test_compact_propagates_close_after_normal_completion
      closed = false

      result =
        Source.each([nil, 1])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.compact)
          .run_with(Sink.to_a)

      assert_equal [1], result
      assert closed
    end

    def test_compact_propagates_close_after_early_sink_completion
      closed = false

      result =
        Source.each([nil, 1, 2])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.compact)
          .run_with(Sink.first)

      assert_equal 1, result
      assert closed
    end

    def test_compact_cleanup_close_failure_after_success_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.compact)
          .run_with(Sink.to_a)
      end

      assert_equal "close boom", error.message
    end

    def test_compact_cleanup_close_failure_after_early_sink_completion_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_close_raising_flow)
          .via(Flow.compact)
          .run_with(Sink.first)
      end

      assert_equal "close boom", error.message
    end

    def test_compact_upstream_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(Flow.compact)
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_compact_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_raising_flow)
          .via(Flow.compact)
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_compact_closes_upstream_after_upstream_failure
      closed = false

      assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.compact)
          .run_with(Sink.to_a)
      end

      assert closed
    end

    def test_compact_ignores_supplied_block
      called = false

      result =
        Source.each([1, nil, 2])
          .via(Flow.compact { called = true })
          .run_with(Sink.to_a)

      assert_equal [1, 2], result
      refute called
    end
  end
end
