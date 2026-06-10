# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowTakeWhileTest < Minitest::Test
    include FlowTestHelpers

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
        Sink.build do |stream|
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
        Sink.build do |stream|
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

    def test_take_while_empty_source_emits_nothing
      result =
        Source.each([])
          .via(Flow.take_while { true })
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_take_while_predicate_true_for_all_elements_emits_entire_stream
      result =
        Source.each([1, 2, 3])
          .via(Flow.take_while { true })
          .run_with(Sink.to_a)

      assert_equal [1, 2, 3], result
    end

    private

    def explode(_value)
      raise "boom"
    end
  end
end
