# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowRejectTest < Minitest::Test
    include FlowTestHelpers

    def test_reject_drops_truthy_predicate_results
      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.reject(&:even?))
          .run_with(Sink.to_a)

      assert_equal [1, 3], result
    end

    def test_reject_preserves_retained_output_order
      result =
        Source.each([3, 1, 4, 2])
          .via(Flow.reject(&:even?))
          .run_with(Sink.to_a)

      assert_equal [3, 1], result
    end

    def test_reject_keeps_false_and_nil_predicate_results
      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.reject do |value|
            next false if value == 1
            next nil if value == 2

            true
          end)
          .run_with(Sink.to_a)

      assert_equal [1, 2], result
    end

    def test_reject_drops_non_boolean_truthy_predicate_results
      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.reject do |value|
            next 0 if value == 1
            next :drop if value == 2

            false
          end)
          .run_with(Sink.to_a)

      assert_equal [3, 4], result
    end

    def test_reject_preserves_retained_object_identity
      first = Object.new
      second = Object.new
      third = Object.new

      result =
        Source.each([first, second, third])
          .via(Flow.reject { |value| value.equal?(second) })
          .run_with(Sink.to_a)

      assert_equal 2, result.size
      assert_same first, result[0]
      assert_same third, result[1]
    end

    def test_reject_pulls_until_non_match_for_each_downstream_demand
      predicate_calls = 0

      result =
        Source.each([2, 1, 3, 4])
          .via(Flow.reject do |value|
            predicate_calls += 1
            value.even?
          end)
          .run_with(Sink.first)

      assert_equal 1, result
      assert_equal 2, predicate_calls
    end

    def test_reject_does_not_pull_upstream_again_after_completion
      next_calls = 0
      flow = build_next_counting_flow { next_calls += 1 }
      sink = build_repeated_pull_sink(2)

      Source.each([1])
        .via(flow)
        .via(Flow.reject { true })
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_reject_propagates_close_after_normal_completion
      closed = false
      flow = build_close_tracking_flow { closed = true }

      result =
        Source.each([1])
          .via(flow)
          .via(Flow.reject { false })
          .run_with(Sink.to_a)

      assert_equal [1], result
      assert closed
    end

    def test_reject_propagates_close_after_early_sink_completion
      closed = false
      flow = build_close_tracking_flow { closed = true }

      result =
        Source.each([1, 2, 3])
          .via(flow)
          .via(Flow.reject { false })
          .run_with(Sink.first)

      assert_equal 1, result
      assert closed
    end

    def test_reject_cleanup_close_failure_after_success_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.reject { false })
          .run_with(Sink.to_a)
      end

      assert_equal "close boom", error.message
    end

    def test_reject_cleanup_close_failure_after_early_sink_completion_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_close_raising_flow)
          .via(Flow.reject { false })
          .run_with(Sink.first)
      end

      assert_equal "close boom", error.message
    end

    def test_reject_exception_fails_stream
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(Flow.reject { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_reject_requires_block
      error = assert_raises(ArgumentError) do
        Flow.reject
      end

      assert_match(/missing block/, error.message)
    end

    def test_reject_empty_source_emits_nothing
      result =
        Source.each([])
          .via(Flow.reject { true })
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_reject_all_rejected_elements_completes_without_emitting
      predicate_calls = 0

      result =
        Source.each([1, 2, 3])
          .via(Flow.reject do |_value|
            predicate_calls += 1
            true
          end)
          .run_with(Sink.to_a)

      assert_equal [], result
      assert_equal 3, predicate_calls
    end

    def test_reject_is_lazy
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.reject { false })

      refute pulled
    end

    def test_reject_pulls_entire_upstream_when_everything_rejected
      next_calls = 0
      sink = build_repeated_pull_sink(2)

      result =
        Source.each([1, 2])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.reject { true })
          .run_with(sink)

      assert_equal 2, result.size
      assert(result.all? { |value| Pull.done?(value) })
      assert_equal 3, next_calls
    end

    def test_reject_upstream_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(Flow.reject { false })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_reject_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_raising_flow)
          .via(Flow.reject { false })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_reject_exception_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.reject { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_reject_closes_upstream_after_upstream_failure
      closed = false

      assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.reject { false })
          .run_with(Sink.to_a)
      end

      assert closed
    end

    def test_reject_closes_upstream_after_predicate_failure
      closed = false

      assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.reject { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert closed
    end

    private

    def explode(_value)
      raise "boom"
    end
  end
end
