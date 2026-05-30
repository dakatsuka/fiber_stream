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

    private

    def explode(_value)
      raise "boom"
    end
  end
end
