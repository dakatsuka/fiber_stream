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
  end
end
