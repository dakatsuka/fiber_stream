# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowMapTest < Minitest::Test
    include FlowTestHelpers

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

    private

    def explode(_value)
      raise "boom"
    end
  end
end
