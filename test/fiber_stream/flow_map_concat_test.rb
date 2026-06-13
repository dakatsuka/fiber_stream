# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowMapConcatTest < Minitest::Test
    include FlowTestHelpers

    def test_map_concat_expands_each_input_in_order
      result =
        Source.each([1, 3])
          .via(Flow.map_concat { |count| 1..count })
          .run_with(Sink.to_a)

      assert_equal [1, 1, 2, 3], result
    end

    def test_map_concat_empty_expansions_emit_no_output
      result =
        Source.each(["a b", "", "c"])
          .via(Flow.map_concat { |line| line.empty? ? [] : line.split })
          .run_with(Sink.to_a)

      assert_equal ["a", "b", "c"], result
    end

    def test_map_concat_flattens_one_level_only
      result =
        Source.each([1])
          .via(Flow.map_concat { [[1, 2], [3, 4]] })
          .run_with(Sink.to_a)

      assert_equal [[1, 2], [3, 4]], result
    end

    def test_map_concat_emits_falsey_values_yielded_by_expansion
      result =
        Source.each([1])
          .via(Flow.map_concat { [nil, false, true] })
          .run_with(Sink.to_a)

      assert_equal [nil, false, true], result
    end

    def test_map_concat_accepts_block_yielding_each_without_enumerable
      result =
        Source.each([1])
          .via(Flow.map_concat { YieldOnlyEach.new(["a", "b"]) })
          .run_with(Sink.to_a)

      assert_equal ["a", "b"], result
    end

    def test_map_concat_enumerates_hashes_with_normal_hash_each
      result =
        Source.each([1])
          .via(Flow.map_concat { { a: 1, b: 2 } })
          .run_with(Sink.to_a)

      assert_equal [[:a, 1], [:b, 2]], result
    end

    def test_map_concat_supports_explicit_string_enumerators
      result =
        Source.each(["ab"])
          .via(Flow.map_concat(&:each_char))
          .run_with(Sink.to_a)

      assert_equal ["a", "b"], result
    end

    def test_map_concat_rejects_string_results
      error = assert_raises(TypeError) do
        Source.each(["ab"])
          .via(Flow.map_concat { |text| text })
          .run_with(Sink.to_a)
      end

      assert_match(/respond to each/, error.message)
    end

    def test_map_concat_rejects_nil_false_and_non_each_results
      [nil, false, Object.new].each do |result|
        error = assert_raises(TypeError) do
          Source.each([1])
            .via(Flow.map_concat { result })
            .run_with(Sink.to_a)
        end

        assert_match(/respond to each/, error.message)
      end
    end

    def test_map_concat_result_each_must_accept_no_arguments
      error = assert_raises(ArgumentError) do
        Source.each([1])
          .via(Flow.map_concat { RequiredArgumentEach.new })
          .run_with(Sink.to_a)
      end

      assert_match(/wrong number of arguments/, error.message)
    end

    def test_map_concat_is_lazy
      pulled = false
      transformed = false

      Source.each([1])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.map_concat do |value|
          transformed = true
          [value]
        end)

      refute pulled
      refute transformed
    end

    def test_map_concat_calls_block_once_per_expanded_input
      calls = []

      result =
        Source.each([1, 2])
          .via(Flow.map_concat do |value|
            calls << value
            [value, value]
          end)
          .run_with(Sink.to_a)

      assert_equal [1, 1, 2, 2], result
      assert_equal [1, 2], calls
    end

    def test_map_concat_demand_returns_after_one_inner_output_without_overpulling
      next_calls = 0

      result =
        Source.each([1, 2])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.map_concat { |value| [value, value + 10] })
          .run_with(Sink.first)

      assert_equal 1, result
      assert_equal 1, next_calls
    end

    def test_map_concat_one_demand_can_skip_empty_expansions
      next_calls = 0
      transform_calls = []

      result =
        Source.each([1, 2, 3])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.map_concat do |value|
            transform_calls << value
            value < 3 ? [] : [value]
          end)
          .run_with(Sink.first)

      assert_equal 3, result
      assert_equal 3, next_calls
      assert_equal [1, 2, 3], transform_calls
    end

    def test_map_concat_infinite_expansion_prevents_later_upstream_pulls
      next_calls = 0

      result =
        Source.each([1, 2])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.map_concat { 1.step })
          .via(Flow.take(3))
          .run_with(Sink.to_a)

      assert_equal [1, 2, 3], result
      assert_equal 1, next_calls
    end

    def test_map_concat_does_not_pull_upstream_again_after_completion
      next_calls = 0
      sink = build_repeated_pull_sink(3)

      Source.each([1])
        .via(build_next_counting_flow { next_calls += 1 })
        .via(Flow.map_concat { [] })
        .run_with(sink)

      assert_equal 2, next_calls
    end

    def test_map_concat_propagates_close_after_normal_completion
      closed = false

      result =
        Source.each([1])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.map_concat { |value| [value] })
          .run_with(Sink.to_a)

      assert_equal [1], result
      assert closed
    end

    def test_map_concat_early_completion_does_not_drain_active_expansion
      yielded = []
      next_calls = 0

      result =
        Source.each([1, 2])
          .via(build_next_counting_flow { next_calls += 1 })
          .via(Flow.map_concat { TrackingEnumerable.new([10, 20, 30], yielded) })
          .run_with(Sink.first)

      assert_equal 10, result
      assert_equal [10], yielded
      assert_equal 1, next_calls
    end

    def test_map_concat_does_not_close_block_results
      expansion = CloseableEnumerable.new([1, 2])

      result =
        Source.each([1])
          .via(Flow.map_concat { expansion })
          .run_with(Sink.first)

      assert_equal 1, result
      refute expansion.closed?
    end

    def test_map_concat_cleanup_close_failure_after_success_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.map_concat { |value| [value] })
          .run_with(Sink.to_a)
      end

      assert_equal "close boom", error.message
    end

    def test_map_concat_cleanup_close_failure_after_early_sink_completion_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.map_concat { |value| [value, value + 1] })
          .run_with(Sink.first)
      end

      assert_equal "close boom", error.message
    end

    def test_map_concat_transform_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(Flow.map_concat { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_map_concat_transform_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .via(Flow.map_concat { |value| explode(value) })
          .run_with(Sink.to_a)
      end

      assert_equal "boom", error.message
    end

    def test_map_concat_inner_enumeration_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(Flow.map_concat { RaisingEach.new })
          .run_with(Sink.to_a)
      end

      assert_equal "each boom", error.message
    end

    def test_map_concat_stop_iteration_from_external_iterator_completes_current_expansion
      result =
        Source.each([1, 2])
          .via(Flow.map_concat { StopIterationEach.new })
          .run_with(Sink.to_a)

      assert_equal [1, 1], result
    end

    def test_map_concat_upstream_failure_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(Flow.map_concat { |value| [value] })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_map_concat_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_next_raising_flow(raise_on_call: 1))
          .via(build_close_raising_flow)
          .via(Flow.map_concat { |value| [value] })
          .run_with(Sink.to_a)
      end

      assert_equal "next boom", error.message
    end

    def test_map_concat_requires_block
      error = assert_raises(ArgumentError) do
        Flow.map_concat
      end

      assert_match(/missing block/, error.message)
    end

    private

    def explode(_value)
      raise "boom"
    end

    class YieldOnlyEach
      def initialize(values)
        @values = values
      end

      def each(&block)
        @values.each(&block)
      end
    end

    class RequiredArgumentEach
      def each(_argument)
        raise "unused"
      end
    end

    class TrackingEnumerable
      def initialize(values, yielded)
        @values = values
        @yielded = yielded
      end

      def each
        @values.each do |value|
          @yielded << value
          yield value
        end
      end
    end

    class CloseableEnumerable
      def initialize(values)
        @values = values
        @closed = false
      end

      def each(&block)
        @values.each(&block)
      end

      def close
        @closed = true
      end

      def closed?
        @closed
      end
    end

    class RaisingEach
      def each
        yield 1
        raise "each boom"
      end
    end

    class StopIterationEach
      def each
        yield 1
        raise StopIteration
      end
    end
  end
end
