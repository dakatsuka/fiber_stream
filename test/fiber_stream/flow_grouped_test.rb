# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowGroupedTest < Minitest::Test
    include FlowTestHelpers

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
  end
end
