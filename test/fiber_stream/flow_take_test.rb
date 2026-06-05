# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/flow_test_helpers"

module FiberStream
  class FlowTakeTest < Minitest::Test
    include FlowTestHelpers

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
  end
end
