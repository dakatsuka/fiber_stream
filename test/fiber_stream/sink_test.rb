# frozen_string_literal: true

require_relative "../test_helper"

module FiberStream
  class SinkTest < Minitest::Test
    def test_to_a_collects_empty_source
      assert_equal [], Source.each([]).run_with(Sink.to_a)
    end

    def test_to_a_preserves_arbitrary_object_values
      object = Object.new

      result = Source.each([object]).run_with(Sink.to_a)

      assert_same object, result.fetch(0)
    end

    def test_first_returns_first_element
      assert_equal 1, Source.each([1, 2, 3]).run_with(Sink.first)
    end

    def test_first_pulls_at_most_one_element
      pulled = 0

      result =
        Source.each([1, 2, 3])
          .via(Flow.map do |value|
            pulled += 1
            value
          end)
          .run_with(Sink.first)

      assert_equal 1, result
      assert_equal 1, pulled
    end

    def test_first_returns_nil_for_empty_source
      assert_nil Source.each([]).run_with(Sink.first)
    end

    def test_count_returns_number_of_elements
      assert_equal 3, Source.each([1, 2, 3]).run_with(Sink.count)
    end

    def test_count_returns_zero_for_empty_source
      assert_equal 0, Source.each([]).run_with(Sink.count)
    end

    def test_count_counts_falsey_elements
      assert_equal 3, Source.each([nil, false, 0]).run_with(Sink.count)
    end

    def test_count_composes_with_flows
      result =
        Source.each([1, 2, 3, 4])
          .select(&:even?)
          .run_with(Sink.count)

      assert_equal 2, result
    end

    def test_count_consumes_complete_stream
      pulled = 0

      result =
        Source.each([1, 2, 3])
          .map do |value|
            pulled += 1
            value
          end
          .run_with(Sink.count)

      assert_equal 3, result
      assert_equal 3, pulled
    end

    def test_count_is_lazy
      called = false

      Source.each([1])
        .map do |value|
          called = true
          value
        end
        .to(Sink.count)

      refute called
    end

    def test_find_returns_first_matching_element
      result =
        Source.each([1, 2, 3, 4])
          .run_with(Sink.find(&:even?))

      assert_equal 2, result
    end

    def test_find_returns_nil_when_no_element_matches
      result =
        Source.each([1, 3, 5])
          .run_with(Sink.find(&:even?))

      assert_nil result
    end

    def test_find_returns_nil_for_empty_source
      assert_nil Source.each([]).run_with(Sink.find { true })
    end

    def test_find_returns_original_element_not_predicate_result
      object = Object.new

      result =
        Source.each([object])
          .run_with(Sink.find { :matched })

      assert_same object, result
    end

    def test_find_uses_ruby_truthiness_for_predicate_results
      calls = []

      result =
        Source.each([1, 2, 3])
          .run_with(
            Sink.find do |value|
              calls << value
              next false if value == 1
              next nil if value == 2

              "truthy"
            end
          )

      assert_equal 3, result
      assert_equal [1, 2, 3], calls
    end

    def test_find_can_return_matching_nil_element
      calls = []

      result =
        Source.each([nil, 1])
          .run_with(
            Sink.find do |value|
              calls << value
              true
            end
          )

      assert_nil result
      assert_equal [nil], calls
    end

    def test_find_can_return_matching_false_element
      result =
        Source.each([false, 1])
          .run_with(Sink.find { true })

      assert_same false, result
    end

    def test_find_stops_pulling_after_match
      pulled = 0

      result =
        Source.each([1, 2, 3])
          .map do |value|
            pulled += 1
            value
          end
          .run_with(Sink.find { |value| value == 2 })

      assert_equal 2, result
      assert_equal 2, pulled
    end

    def test_find_is_lazy
      called = false

      Source.each([1])
        .map do |value|
          called = true
          value
        end
        .to(Sink.find { true })

      refute called
    end

    def test_find_requires_block
      error = assert_raises(ArgumentError) do
        Sink.find
      end

      assert_match(/missing block/, error.message)
    end

    def test_find_uses_identity_completion_semantics
      object = EqualToEverything.new

      result =
        Source.each([object])
          .run_with(Sink.find { true })

      assert_same object, result
    end

    def test_find_exception_fails_stream
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .run_with(Sink.find { |value| raise_find_boom(value) })
      end

      assert_equal "find boom", error.message
    end

    def test_find_does_not_pull_after_block_raises
      pulled = 0

      error = assert_raises(RuntimeError) do
        Source.each([1, 2, 3])
          .map do |value|
            pulled += 1
            value
          end
          .run_with(
            Sink.find do |value|
              raise "find boom" if value == 2

              false
            end
          )
      end

      assert_equal "find boom", error.message
      assert_equal 2, pulled
    end

    def test_find_closes_flow_chain_when_block_raises
      closed = false
      flow = build_close_tracking_flow { closed = true }

      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(flow)
          .run_with(Sink.find { |value| raise_find_boom(value) })
      end

      assert_equal "find boom", error.message
      assert closed
    end

    def test_find_cleanup_close_failure_after_match_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_close_raising_flow)
          .run_with(Sink.find { true })
      end

      assert_equal "close boom", error.message
    end

    def test_find_cleanup_close_failure_after_no_match_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .run_with(Sink.find { false })
      end

      assert_equal "close boom", error.message
    end

    def test_find_predicate_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .run_with(Sink.find { |value| raise_find_boom(value) })
      end

      assert_equal "find boom", error.message
    end

    def test_find_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .map { |value| raise_upstream_boom(value) }
          .run_with(Sink.find { true })
      end

      assert_equal "upstream boom", error.message
    end

    def test_any_returns_true_when_an_element_matches
      result =
        Source.each([1, 2, 3, 4])
          .run_with(Sink.any?(&:even?))

      assert_same true, result
    end

    def test_any_returns_false_when_no_element_matches
      result =
        Source.each([1, 3, 5])
          .run_with(Sink.any?(&:even?))

      assert_same false, result
    end

    def test_any_returns_false_for_empty_source
      assert_same false, Source.each([]).run_with(Sink.any? { true })
    end

    def test_any_returns_boolean_not_predicate_result
      result =
        Source.each([1])
          .run_with(Sink.any? { :matched })

      assert_same true, result
    end

    def test_any_uses_ruby_truthiness_for_predicate_results
      calls = []

      result =
        Source.each([1, 2, 3])
          .run_with(
            Sink.any? do |value|
              calls << value
              next false if value == 1
              next nil if value == 2

              "truthy"
            end
          )

      assert_same true, result
      assert_equal [1, 2, 3], calls
    end

    def test_any_can_match_nil_element
      calls = []

      result =
        Source.each([nil, 1])
          .run_with(
            Sink.any? do |value|
              calls << value
              true
            end
          )

      assert_same true, result
      assert_equal [nil], calls
    end

    def test_any_can_match_false_element
      result =
        Source.each([false, 1])
          .run_with(Sink.any? { true })

      assert_same true, result
    end

    def test_any_stops_pulling_after_match
      pulled = 0

      result =
        Source.each([1, 2, 3])
          .map do |value|
            pulled += 1
            value
          end
          .run_with(Sink.any? { |value| value == 2 })

      assert_same true, result
      assert_equal 2, pulled
    end

    def test_any_is_lazy
      called = false

      Source.each([1])
        .map do |value|
          called = true
          value
        end
        .to(Sink.any? { true })

      refute called
    end

    def test_any_does_not_call_predicate_during_construction
      called = false

      sink = Sink.any? { called = true }

      assert_instance_of Sink, sink
      refute called
    end

    def test_any_requires_block
      error = assert_raises(ArgumentError) do
        Sink.any?
      end

      assert_match(/missing block/, error.message)
    end

    def test_any_uses_identity_completion_semantics
      object = EqualToEverything.new

      result =
        Source.each([object])
          .run_with(Sink.any? { true })

      assert_same true, result
    end

    def test_any_exception_fails_stream
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .run_with(Sink.any? { |value| raise_any_boom(value) })
      end

      assert_equal "any boom", error.message
    end

    def test_any_does_not_pull_after_block_raises
      pulled = 0

      error = assert_raises(RuntimeError) do
        Source.each([1, 2, 3])
          .map do |value|
            pulled += 1
            value
          end
          .run_with(
            Sink.any? do |value|
              raise "any boom" if value == 2

              false
            end
          )
      end

      assert_equal "any boom", error.message
      assert_equal 2, pulled
    end

    def test_any_closes_flow_chain_when_block_raises
      closed = false
      flow = build_close_tracking_flow { closed = true }

      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(flow)
          .run_with(Sink.any? { |value| raise_any_boom(value) })
      end

      assert_equal "any boom", error.message
      assert closed
    end

    def test_any_cleanup_close_failure_after_true_result_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1, 2])
          .via(build_close_raising_flow)
          .run_with(Sink.any? { true })
      end

      assert_equal "close boom", error.message
    end

    def test_any_cleanup_close_failure_after_false_result_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .run_with(Sink.any? { false })
      end

      assert_equal "close boom", error.message
    end

    def test_any_predicate_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .run_with(Sink.any? { |value| raise_any_boom(value) })
      end

      assert_equal "any boom", error.message
    end

    def test_any_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .map { |value| raise_upstream_boom(value) }
          .run_with(Sink.any? { true })
      end

      assert_equal "upstream boom", error.message
    end

    def test_all_returns_true_when_all_elements_match
      result =
        Source.each([2, 4, 6])
          .run_with(Sink.all?(&:even?))

      assert_same true, result
    end

    def test_all_returns_false_when_an_element_does_not_match
      result =
        Source.each([2, 3, 4])
          .run_with(Sink.all?(&:even?))

      assert_same false, result
    end

    def test_all_returns_true_for_empty_source
      assert_same true, Source.each([]).run_with(Sink.all? { false })
    end

    def test_all_returns_boolean_not_predicate_result
      result =
        Source.each([1])
          .run_with(Sink.all? { :matched })

      assert_same true, result
    end

    def test_all_uses_ruby_truthiness_for_predicate_results
      calls = []

      result =
        Source.each([1, 2, 3])
          .run_with(
            Sink.all? do |value|
              calls << value
              next "truthy" if value == 1
              next true if value == 2

              nil
            end
          )

      assert_same false, result
      assert_equal [1, 2, 3], calls
    end

    def test_all_can_reject_nil_element
      calls = []

      result =
        Source.each([nil, 1])
          .run_with(
            Sink.all? do |value|
              calls << value
              value
            end
          )

      assert_same false, result
      assert_equal [nil], calls
    end

    def test_all_can_reject_false_element
      result =
        Source.each([false, 1])
          .run_with(Sink.all? { |value| value })

      assert_same false, result
    end

    def test_all_stops_pulling_after_non_match
      pulled = 0

      result =
        Source.each([2, 3, 4])
          .map do |value|
            pulled += 1
            value
          end
          .run_with(Sink.all?(&:even?))

      assert_same false, result
      assert_equal 2, pulled
    end

    def test_all_is_lazy
      called = false

      Source.each([1])
        .map do |value|
          called = true
          value
        end
        .to(Sink.all? { true })

      refute called
    end

    def test_all_does_not_call_predicate_during_construction
      called = false

      sink = Sink.all? { called = true }

      assert_instance_of Sink, sink
      refute called
    end

    def test_all_requires_block
      error = assert_raises(ArgumentError) do
        Sink.all?
      end

      assert_match(/missing block/, error.message)
    end

    def test_all_uses_identity_completion_semantics
      object = EqualToEverything.new

      result =
        Source.each([object])
          .run_with(Sink.all? { true })

      assert_same true, result
    end

    def test_all_exception_fails_stream
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .run_with(Sink.all? { |value| raise_all_boom(value) })
      end

      assert_equal "all boom", error.message
    end

    def test_all_does_not_pull_after_block_raises
      pulled = 0

      error = assert_raises(RuntimeError) do
        Source.each([1, 2, 3])
          .map do |value|
            pulled += 1
            value
          end
          .run_with(
            Sink.all? do |value|
              raise "all boom" if value == 2

              true
            end
          )
      end

      assert_equal "all boom", error.message
      assert_equal 2, pulled
    end

    def test_all_closes_flow_chain_when_block_raises
      closed = false
      flow = build_close_tracking_flow { closed = true }

      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(flow)
          .run_with(Sink.all? { |value| raise_all_boom(value) })
      end

      assert_equal "all boom", error.message
      assert closed
    end

    def test_all_cleanup_close_failure_after_true_result_propagates
      error = assert_raises(RuntimeError) do
        Source.each([2, 4])
          .via(build_close_raising_flow)
          .run_with(Sink.all?(&:even?))
      end

      assert_equal "close boom", error.message
    end

    def test_all_cleanup_close_failure_after_false_result_propagates
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .run_with(Sink.all? { false })
      end

      assert_equal "close boom", error.message
    end

    def test_all_predicate_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .run_with(Sink.all? { |value| raise_all_boom(value) })
      end

      assert_equal "all boom", error.message
    end

    def test_all_upstream_failure_wins_over_cleanup_close_failure
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(build_close_raising_flow)
          .map { |value| raise_upstream_boom(value) }
          .run_with(Sink.all? { true })
      end

      assert_equal "upstream boom", error.message
    end

    def test_fold_returns_final_accumulator
      result =
        Source.each([1, 2, 3])
          .run_with(Sink.fold(0) { |accumulator, value| accumulator + value })

      assert_equal 6, result
    end

    def test_fold_returns_initial_accumulator_for_empty_source
      initial = Object.new

      result = Source.each([]).run_with(Sink.fold(initial) { :unused })

      assert_same initial, result
    end

    def test_fold_composes_with_flows
      result =
        Source.each([1, 2, 3, 4])
          .via(Flow.select(&:even?))
          .via(Flow.map { |value| value * 10 })
          .run_with(Sink.fold([]) { |values, value| values + [value] })

      assert_equal [20, 40], result
    end

    def test_fold_exception_fails_stream
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .run_with(Sink.fold(0) { raise "fold boom" })
      end

      assert_equal "fold boom", error.message
    end

    def test_fold_closes_flow_chain_when_block_raises
      closed = false
      flow = build_close_tracking_flow { closed = true }

      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(flow)
          .run_with(Sink.fold(0) { raise "fold boom" })
      end

      assert_equal "fold boom", error.message
      assert closed
    end

    def test_fold_requires_block
      error = assert_raises(ArgumentError) do
        Sink.fold(0)
      end

      assert_match(/missing block/, error.message)
    end

    def test_foreach_runs_block_for_each_element_and_returns_count
      handled = []

      result =
        Source.each([1, 2, 3])
          .run_with(Sink.foreach { |value| handled << value })

      assert_equal 3, result
      assert_equal [1, 2, 3], handled
    end

    def test_foreach_returns_zero_for_empty_source
      handled = []

      result =
        Source.each([])
          .run_with(Sink.foreach { |value| handled << value })

      assert_equal 0, result
      assert_empty handled
    end

    def test_foreach_is_lazy
      called = false

      Source.each([1])
        .to(Sink.foreach { called = true })

      refute called
    end

    def test_foreach_requires_block
      error = assert_raises(ArgumentError) do
        Sink.foreach
      end

      assert_match(/missing block/, error.message)
    end

    def test_foreach_exception_fails_stream
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .run_with(Sink.foreach { raise "foreach boom" })
      end

      assert_equal "foreach boom", error.message
    end

    def test_foreach_does_not_pull_after_block_raises
      pulled = 0

      error = assert_raises(RuntimeError) do
        Source.each([1, 2, 3])
          .map do |value|
            pulled += 1
            value
          end
          .run_with(
            Sink.foreach do |value|
              raise "foreach boom" if value == 2
            end
          )
      end

      assert_equal "foreach boom", error.message
      assert_equal 2, pulled
    end

    def test_foreach_closes_flow_chain_when_block_raises
      closed = false
      flow = build_close_tracking_flow { closed = true }

      error = assert_raises(RuntimeError) do
        Source.each([1])
          .via(flow)
          .run_with(Sink.foreach { raise "foreach boom" })
      end

      assert_equal "foreach boom", error.message
      assert closed
    end

    def test_to_a_single_element
      assert_equal [42], Source.each([42]).run_with(Sink.to_a)
    end

    def test_fold_single_element_returns_reduced_value
      result =
        Source.each([5])
          .run_with(Sink.fold(0) { |accumulator, value| accumulator + value })

      assert_equal 5, result
    end

    def test_first_on_single_element_stream
      assert_equal 42, Source.each([42]).run_with(Sink.first)
    end

    private

    def build_close_tracking_flow(&on_close)
      Flow.build do |upstream|
        CloseTrackingStage.new(upstream, &on_close)
      end
    end

    def build_close_raising_flow
      Flow.build do |upstream|
        CloseRaisingStage.new(upstream)
      end
    end

    def raise_find_boom(_value)
      raise "find boom"
    end

    def raise_any_boom(_value)
      raise "any boom"
    end

    def raise_all_boom(_value)
      raise "all boom"
    end

    def raise_upstream_boom(_value)
      raise "upstream boom"
    end

    class EqualToEverything
      def ==(_other)
        true
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
      def initialize(upstream)
        @upstream = upstream
        @closed = false
      end

      def next
        @upstream.next
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
        raise "close boom"
      end
    end
  end
end
