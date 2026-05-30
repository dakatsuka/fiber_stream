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
  end
end
