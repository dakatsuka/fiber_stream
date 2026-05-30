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

    private

    def build_close_tracking_flow(&on_close)
      Flow.__send__(:new) do |upstream|
        CloseTrackingStage.new(upstream, &on_close)
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
  end
end
