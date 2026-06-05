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

    def test_grouped_emits_full_groups_and_final_partial_group
      result =
        Source.each([1, 2, 3, 4, 5])
          .via(Flow.grouped(2))
          .run_with(Sink.to_a)

      assert_equal [[1, 2], [3, 4], [5]], result
    end

    def test_grouped_source_convenience
      result =
        Source.each([1, 2, 3])
          .grouped(2)
          .run_with(Sink.to_a)

      assert_equal [[1, 2], [3]], result
    end

    def test_grouped_one_wraps_each_element
      result =
        Source.each(["a", "b"])
          .via(Flow.grouped(1))
          .run_with(Sink.to_a)

      assert_equal [["a"], ["b"]], result
    end

    def test_grouped_empty_source_emits_no_empty_group
      result =
        Source.each([])
          .via(Flow.grouped(3))
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_grouped_is_lazy
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.grouped(2))

      refute pulled
    end

    def test_grouped_each_demand_pulls_at_most_count_values
      next_calls = 0

      result =
        Source.each([1, 2, 3])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.grouped(2))
          .run_with(Sink.first)

      assert_equal [1, 2], result
      assert_equal 2, next_calls
    end

    def test_grouped_exact_full_group_does_not_probe_for_completion
      next_calls = 0
      sink =
        Sink.__send__(:new) do |stream|
          first = stream.next
          calls_after_first_pull = next_calls
          second = stream.next

          [first, calls_after_first_pull, second]
        end

      result =
        Source.each([1, 2])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.grouped(2))
          .run_with(sink)

      assert_equal [1, 2], result.fetch(0)
      assert_equal 2, result.fetch(1)
      assert Pull.done?(result.fetch(2))
      assert_equal 3, next_calls
    end

    def test_grouped_does_not_pull_upstream_again_after_completion
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Source.each([1])
        .via(build_next_counting_flow { next_calls += 1 })
        .via(Flow.grouped(2))
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_grouped_emits_distinct_mutable_arrays
      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.grouped(2))
          .run_with(Sink.to_a)

      refute_same result.fetch(0), result.fetch(1)

      result.fetch(0) << :changed
      assert_equal [3, 4], result.fetch(1)
    end

    def test_grouped_preserves_element_identity
      first = Object.new
      second = Object.new

      result =
        Source.each([first, second])
          .via(Flow.grouped(2))
          .run_with(Sink.to_a)

      assert_same first, result.fetch(0).fetch(0)
      assert_same second, result.fetch(0).fetch(1)
    end

    def test_grouped_propagates_close_after_early_sink_completion
      closed = false

      result =
        Source.each([1, 2, 3])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.grouped(2))
          .run_with(Sink.first)

      assert_equal [1, 2], result
      assert closed
    end

    def test_grouped_upstream_failure_discards_partial_group_and_propagates
      seen_groups = []
      sink =
        Sink.__send__(:new) do |stream|
          loop do
            value = stream.next
            break if Pull.done?(value)

            seen_groups << value
          end
        end

      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_next_raising_flow(raise_on_call: 2))
          .via(Flow.grouped(2))
          .run_with(sink)
      end

      assert_equal "next boom", error.message
      assert_equal [], seen_groups
    end

    def test_grouped_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_raising_flow)
          .via(Flow.grouped(2))
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_grouped_closes_upstream_after_upstream_failure
      closed = false

      assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.grouped(2))
          .run_with(Sink.to_a)
      end

      assert closed
    end

    def test_grouped_rejects_zero_count
      error = assert_raises(ArgumentError) do
        Flow.grouped(0)
      end

      assert_match(/count must be positive/, error.message)
    end

    def test_grouped_rejects_negative_count
      error = assert_raises(ArgumentError) do
        Flow.grouped(-1)
      end

      assert_match(/count must be positive/, error.message)
    end

    def test_grouped_rejects_non_integer_count
      error = assert_raises(TypeError) do
        Flow.grouped(2.0)
      end

      assert_match(/count must be an Integer/, error.message)
    end

    def test_grouped_does_not_coerce_count
      integer_like = Object.new

      def integer_like.to_int
        2
      end

      error = assert_raises(TypeError) do
        Flow.grouped(integer_like)
      end

      assert_match(/count must be an Integer/, error.message)
    end

    def test_take_while_emits_truthy_prefix
      result =
        Source.each([1, 2, 3, 1])
          .via(Flow.take_while { |value| value < 3 })
          .run_with(Sink.to_a)

      assert_equal [1, 2], result
    end

    def test_take_while_does_not_emit_terminating_element
      result =
        Source.each([1, 2, 3])
          .via(Flow.take_while(&:odd?))
          .run_with(Sink.to_a)

      assert_equal [1], result
    end

    def test_take_while_false_and_nil_terminate
      false_result =
        Source.each([1, 2])
          .via(Flow.take_while { false })
          .run_with(Sink.to_a)
      nil_result =
        Source.each([1, 2])
          .via(Flow.take_while { nil })
          .run_with(Sink.to_a)

      assert_equal [], false_result
      assert_equal [], nil_result
    end

    def test_take_while_completes_when_upstream_completes_first
      result =
        Source.each([1, 2])
          .via(Flow.take_while { true })
          .run_with(Sink.to_a)

      assert_equal [1, 2], result
    end

    def test_take_while_pulls_at_most_one_upstream_value_per_demand
      next_calls = 0

      result =
        Source.each([1, 2, 3])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.take_while { |value| value < 3 })
          .run_with(Sink.first)

      assert_equal 1, result
      assert_equal 1, next_calls
    end

    def test_take_while_closes_upstream_when_predicate_fails
      closed = false
      sink =
        Sink.__send__(:new) do |stream|
          first = stream.next
          second = stream.next
          [first, second, closed]
        end

      result =
        Source.each([1, 2, 3])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.take_while { |value| value < 2 })
          .run_with(sink)

      assert_equal 1, result.fetch(0)
      assert Pull.done?(result.fetch(1))
      assert result.fetch(2)
    end

    def test_take_while_close_failure_on_predicate_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.take_while { false })
          .run_with(Sink.to_a)
      end

      assert_equal "close boom", error.message
    end

    def test_take_while_close_failure_marks_stage_complete
      next_calls = 0
      close_calls = 0
      sink =
        Sink.__send__(:new) do |stream|
          first_error =
            begin
              stream.next
              nil
            rescue RuntimeError => error
              error
            end
          second = stream.next
          [first_error&.message, second]
        end

      result =
        Source.each([1, 2])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(build_close_raising_flow { close_calls += 1 })
          .via(Flow.take_while { false })
          .run_with(sink)

      assert_equal "close boom", result.fetch(0)
      assert Pull.done?(result.fetch(1))
      assert_equal 1, next_calls
      assert_equal 1, close_calls
    end

    def test_take_while_does_not_pull_upstream_again_after_predicate_failure
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Source.each([1, 2, 3])
        .via(build_next_counting_flow { next_calls += 1 })
        .via(Flow.take_while { |value| value < 2 })
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_take_while_does_not_pull_upstream_again_after_upstream_completion
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Source.each([1])
        .via(build_next_counting_flow { next_calls += 1 })
        .via(Flow.take_while { true })
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_take_while_is_lazy
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.take_while { true })

      refute pulled
    end

    def test_take_while_predicate_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(Flow.take_while { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_take_while_predicate_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.take_while { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_take_while_upstream_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(Flow.take_while { true })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_take_while_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_raising_flow)
          .via(Flow.take_while { true })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_take_while_requires_block
      error = assert_raises(ArgumentError) do
        Flow.take_while
      end

      assert_match(/missing block/, error.message)
    end

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

    def build_close_tracking_flow(&on_close)
      Flow.__send__(:new) do |upstream|
        CloseTrackingStage.new(upstream, &on_close)
      end
    end

    def build_close_raising_flow(&on_close)
      Flow.__send__(:new) do |upstream|
        CloseRaisingStage.new(upstream, &on_close)
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

    class CloseRaisingStage
      def initialize(upstream, &on_close)
        @upstream = upstream
        @on_close = on_close || -> {}
        @closed = false
      end

      def next
        @upstream.next
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
        @on_close.call
        raise "close boom"
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
