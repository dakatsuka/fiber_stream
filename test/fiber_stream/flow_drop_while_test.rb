# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowDropWhileTest < Minitest::Test
    include FlowTestHelpers

    def test_drop_while_drops_truthy_prefix
      result =
        Source.each([1, 2, 3, 1])
          .via(Flow.drop_while { |value| value < 3 })
          .run_with(Sink.to_a)

      assert_equal [3, 1], result
    end

    def test_drop_while_emits_first_falsey_predicate_element
      result =
        Source.each([1, 2, 3])
          .via(Flow.drop_while(&:odd?))
          .run_with(Sink.to_a)

      assert_equal [2, 3], result
    end

    def test_drop_while_false_and_nil_stop_dropping
      false_result =
        Source.each([1, 2])
          .via(Flow.drop_while { false })
          .run_with(Sink.to_a)
      nil_result =
        Source.each([1, 2])
          .via(Flow.drop_while { nil })
          .run_with(Sink.to_a)

      assert_equal [1, 2], false_result
      assert_equal [1, 2], nil_result
    end

    def test_drop_while_completes_when_upstream_completes_before_retained_value
      result =
        Source.each([1, 2])
          .via(Flow.drop_while { true })
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_drop_while_does_not_call_predicate_after_boundary
      predicate_calls = 0

      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.drop_while do |value|
            predicate_calls += 1
            value < 3
          end)
          .run_with(Sink.to_a)

      assert_equal [3, 4], result
      assert_equal 3, predicate_calls
    end

    def test_drop_while_first_demand_pulls_until_first_retained_value
      next_calls = 0

      result =
        Source.each([1, 2, 3, 4])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.drop_while { |value| value < 3 })
          .run_with(Sink.first)

      assert_equal 3, result
      assert_equal 3, next_calls
    end

    def test_drop_while_after_boundary_pulls_one_upstream_value_per_demand
      next_calls = 0
      sink = build_repeated_pull_sink(2)

      result =
        Source.each([1, 2, 3, 4])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.drop_while { |value| value < 3 })
          .run_with(sink)

      assert_equal [3, 4], result
      assert_equal 4, next_calls
    end

    def test_drop_while_does_not_pull_upstream_again_after_completion
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Source.each([1])
        .via(build_next_counting_flow { next_calls += 1 })
        .via(Flow.drop_while { true })
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_drop_while_is_lazy
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.drop_while { true })

      refute pulled
    end

    def test_drop_while_propagates_close_after_early_sink_completion
      closed = false

      result =
        Source.each([1, 2, 3])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.drop_while { |value| value < 2 })
          .run_with(Sink.first)

      assert_equal 2, result
      assert closed
    end

    def test_drop_while_cleanup_close_failure_after_success_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_close_raising_flow)
          .via(Flow.drop_while { |value| value < 2 })
          .run_with(Sink.to_a)
      end

      assert_equal "close boom", error.message
    end

    def test_drop_while_cleanup_close_failure_after_early_sink_completion_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_close_raising_flow)
          .via(Flow.drop_while { |value| value < 2 })
          .run_with(Sink.first)
      end

      assert_equal "close boom", error.message
    end

    def test_drop_while_predicate_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(Flow.drop_while { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_drop_while_predicate_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.drop_while { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_drop_while_upstream_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(Flow.drop_while { true })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_drop_while_dropping_upstream_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_next_raising_flow(raise_on_call: 2))
          .via(Flow.drop_while { true })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_drop_while_pass_through_upstream_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_next_raising_flow(raise_on_call: 2))
          .via(Flow.drop_while { false })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_drop_while_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_raising_flow)
          .via(Flow.drop_while { true })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_drop_while_dropping_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_next_raising_flow(raise_on_call: 2))
          .via(build_close_raising_flow)
          .via(Flow.drop_while { true })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_drop_while_pass_through_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_next_raising_flow(raise_on_call: 2))
          .via(build_close_raising_flow)
          .via(Flow.drop_while { false })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_drop_while_requires_block
      error = assert_raises(ArgumentError) do
        Flow.drop_while
      end

      assert_match(/missing block/, error.message)
    end

    private

    def explode(_value)
      raise "boom"
    end
  end
end
