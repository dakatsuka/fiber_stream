# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowScanTest < Minitest::Test
    include FlowTestHelpers

    def test_scan_emits_running_accumulators
      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.scan(0) { |sum, value| sum + value })
          .run_with(Sink.to_a)

      assert_equal [1, 3, 6, 10], result
    end

    def test_scan_source_convenience
      result =
        Source.each([1, 2, 3])
          .scan(1) { |product, value| product * value }
          .run_with(Sink.to_a)

      assert_equal [1, 2, 6], result
    end

    def test_scan_supports_state_updates
      transitions = {
        idle: { start: :running },
        running: { tick: :running, stop: :stopped }
      }

      result =
        Source.each([:start, :tick, :stop])
          .via(Flow.scan(:idle) { |state, event| transitions.fetch(state).fetch(event) })
          .run_with(Sink.to_a)

      assert_equal [:running, :running, :stopped], result
    end

    def test_scan_empty_source_emits_nothing
      result =
        Source.each([])
          .via(Flow.scan(0) { |sum, value| sum + value })
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_scan_single_element_emits_one_accumulator
      result =
        Source.each([7])
          .via(Flow.scan(0) { |sum, value| sum + value })
          .run_with(Sink.to_a)

      assert_equal [7], result
    end

    def test_scan_final_value_matches_fold_for_non_empty_input_sequence
      values = [1, 2, 3]

      final =
        Source.each(values)
          .run_with(Sink.fold(0) { |sum, value| sum + value })
      running =
        Source.each(values)
          .via(Flow.scan(0) { |sum, value| sum + value })
          .run_with(Sink.to_a)

      assert_equal final, running.last
    end

    def test_scan_is_lazy
      pulled = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.scan(0) { |sum, value| sum + value })

      refute pulled
    end

    def test_scan_each_demand_pulls_at_most_one_immediate_upstream_value
      next_calls = 0

      result =
        Source.each([1, 2])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.scan(0) { |sum, value| sum + value })
          .run_with(Sink.first)

      assert_equal 1, result
      assert_equal 1, next_calls
    end

    def test_scan_does_not_pull_upstream_again_after_completion
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Source.each([1])
        .via(build_next_counting_flow { next_calls += 1 })
        .via(Flow.scan(0) { |sum, value| sum + value })
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_scan_reuses_mutable_accumulator_by_identity_when_block_returns_it
      initial = []

      result =
        Source.each([1, 2, 3])
          .via(Flow.scan(initial) { |values, value| values << value })
          .run_with(Sink.to_a)

      assert_equal [[1, 2, 3], [1, 2, 3], [1, 2, 3]], result
      assert_same initial, result.fetch(0)
      assert_same result.fetch(0), result.fetch(1)
      assert_same result.fetch(1), result.fetch(2)
    end

    def test_scan_does_not_roll_back_in_place_mutation_when_reducer_raises
      initial = []

      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(
            Flow.scan(initial) do |values, value|
              values << value
              raise "scan boom" if value == 2

              values
            end
          )
          .run_with(Sink.to_a)
      end

      assert_equal "scan boom", error.message
      assert_equal [1, 2], initial
    end

    def test_scan_propagates_close_after_early_sink_completion
      closed = false

      result =
        Source.each([1, 2, 3])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.scan(0) { |sum, value| sum + value })
          .run_with(Sink.first)

      assert_equal 1, result
      assert closed
    end

    def test_scan_reducer_failure_propagates_and_closes_upstream
      closed = false

      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.scan(0) { |sum, value| explode(sum, value) })
          .run_with(Sink.to_a)
      end

      assert_equal "scan boom", error.message
      assert closed
    end

    def test_scan_reducer_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.scan(0) { |sum, value| explode(sum, value) })
          .run_with(Sink.to_a)
      end

      assert_equal "scan boom", error.message
    end

    def test_scan_upstream_failure_propagates_without_emitting_for_failing_pull
      seen = []
      sink =
        Sink.build do |stream|
          loop do
            value = stream.next
            break if Pull.done?(value)

            seen << value
          end
        end

      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_next_raising_flow(raise_on_call: 2))
          .via(Flow.scan(0) { |sum, value| sum + value })
          .run_with(sink)
      end

      assert_equal "next boom", error.message
      assert_equal [1], seen
    end

    def test_scan_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_raising_flow)
          .via(Flow.scan(0) { |sum, value| sum + value })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_scan_closes_upstream_after_upstream_failure
      closed = false

      assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.scan(0) { |sum, value| sum + value })
          .run_with(Sink.to_a)
      end

      assert closed
    end

    def test_scan_requires_block
      error = assert_raises(ArgumentError) do
        Flow.scan(0)
      end

      assert_match(/missing block/, error.message)
    end

    def test_scan_source_convenience_requires_block
      error = assert_raises(ArgumentError) do
        Source.each([1]).scan(0)
      end

      assert_match(/missing block/, error.message)
    end

    private

    def explode(_sum, _value)
      raise "scan boom"
    end
  end
end
