# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowTapTest < Minitest::Test
    include FlowTestHelpers

    def test_tap_observes_and_passes_through_elements
      seen = []

      result =
        Source.each([1, 2, 3])
          .via(Flow.tap { |value| seen << value })
          .run_with(Sink.to_a)

      assert_equal [1, 2, 3], result
      assert_equal [1, 2, 3], seen
    end

    def test_tap_composes_in_order
      seen = []

      result =
        Source.each([1, 2])
          .via(Flow.tap { |value| seen << [:before, value] })
          .via(Flow.map { |value| value * 10 })
          .via(Flow.tap { |value| seen << [:after, value] })
          .run_with(Sink.to_a)

      assert_equal [10, 20], result
      assert_equal [[:before, 1], [:after, 10], [:before, 2], [:after, 20]], seen
    end

    def test_tap_block_return_value_is_ignored
      result =
        Source.each([1])
          .via(Flow.tap { "ignored".upcase })
          .run_with(Sink.to_a)

      assert_equal [1], result
    end

    def test_tap_preserves_object_identity
      object = Object.new
      observed = nil

      result =
        Source.each([object])
          .via(Flow.tap { |value| observed = value })
          .run_with(Sink.first)

      assert_same object, observed
      assert_same object, result
    end

    def test_tap_requires_block
      error = assert_raises(ArgumentError) do
        Flow.tap
      end

      assert_match(/missing block/, error.message)
    end

    def test_tap_is_lazy_at_construction
      called = false

      Source.each([1]).via(Flow.tap { called = true })

      refute called
    end

    def test_tap_calls_observer_before_emitting_downstream
      seen = []

      result =
        Source.each([1])
          .via(Flow.tap { |value| seen << value })
          .via(Flow.map { |value| [value, seen.dup] })
          .run_with(Sink.first)

      assert_equal [1, [1]], result
    end

    def test_tap_does_not_observe_upstream_completion
      seen = []

      result =
        Source.each([])
          .via(Flow.tap { |value| seen << value })
          .run_with(Sink.to_a)

      assert_empty result
      assert_empty seen
    end

    def test_tap_does_not_observe_upstream_failure_before_value
      seen = []

      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(Flow.tap { |value| seen << value })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
      assert_empty seen
    end

    def test_tap_observer_exception_fails_stream
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(Flow.tap { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_tap_observer_exception_suppresses_current_value
      consumed = []

      assert_raises(RuntimeError) do
        Source.each([1])
          .via(Flow.tap { |value| explode(value) })
          .run_with(Sink.build do |stream|
            Pull.each_value(stream) { |value| consumed << value }
          end)
      end

      assert_empty consumed
    end

    def test_tap_observer_exception_wins_over_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.tap { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_tap_does_not_pull_upstream_again_after_completion
      next_calls = 0
      flow = build_next_counting_flow { next_calls += 1 }
      sink = build_repeated_pull_sink(3)

      Source.each([1])
        .via(flow)
        .via(Flow.tap { |_value| })
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_tap_early_completion_observes_only_pulled_value_and_closes_upstream
      seen = []
      closed = false

      result =
        Source.each([1, 2])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.tap { |value| seen << value })
          .run_with(Sink.first)

      assert_equal 1, result
      assert_equal [1], seen
      assert closed
    end

    def test_tap_downstream_failure_closes_upstream_and_preserves_sink_failure
      seen = []
      closed = false

      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.tap { |value| seen << value })
          .run_with(failing_after_one_sink)
      end

      assert_equal "sink boom", error.message
      assert_equal [1], seen
      assert closed
    end

    def test_source_tap_remains_object_tap
      source = Source.each([1])
      yielded = nil

      result = source.tap { |value| yielded = value }

      assert_same source, yielded
      assert_same source, result
    end

    private

    def explode(_value)
      raise "boom"
    end

    def failing_after_one_sink
      Sink.build do |stream|
        stream.next
        raise "sink boom"
      end
    end
  end
end
