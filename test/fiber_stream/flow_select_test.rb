# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowSelectTest < Minitest::Test
    include FlowTestHelpers

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

    def test_select_empty_source_emits_nothing
      result =
        Source.each([])
          .via(Flow.select { true })
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_select_all_rejected_elements_completes_without_emitting
      predicate_calls = 0

      result =
        Source.each([1, 2, 3])
          .via(Flow.select do |_value|
            predicate_calls += 1
            false
          end)
          .run_with(Sink.to_a)

      assert_equal [], result
      assert_equal 3, predicate_calls
    end

    def test_select_is_lazy
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.select { true })

      refute pulled
    end

    def test_select_pulls_entire_upstream_when_nothing_matches
      next_calls = 0
      sink = build_repeated_pull_sink(2)

      result =
        Source.each([1, 2])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.select { false })
          .run_with(sink)

      assert_equal 2, result.size
      assert(result.all? { |value| Pull.done?(value) })
      assert_equal 3, next_calls
    end

    def test_select_upstream_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(Flow.select { true })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_select_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_raising_flow)
          .via(Flow.select { true })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_select_exception_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.select { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_select_closes_upstream_after_upstream_failure
      closed = false

      assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.select { true })
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
