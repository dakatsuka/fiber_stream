# frozen_string_literal: true

require_relative "../test_helper"

module FiberStream
  class SourceTest < Minitest::Test
    def test_construction_is_lazy
      called = false

      source =
        Source.each([1])
          .via(Flow.map do |value|
            called = true
            value
          end)

      refute called
      assert_equal [1], source.run_with(Sink.to_a)
      assert called
    end

    def test_each_materializes_an_enumerator_for_each_run
      enumerable = CountingEnumerable.new([1, 2])
      source = Source.each(enumerable)

      assert_equal [1, 2], source.run_with(Sink.to_a)
      assert_equal [1, 2], source.run_with(Sink.to_a)
      assert_equal 2, enumerable.each_calls
    end

    def test_each_supports_enumerables_that_yield_to_a_block
      enumerable = YieldingEnumerable.new([1, 2])

      assert_equal [1, 2], Source.each(enumerable).run_with(Sink.to_a)
    end

    def test_each_does_not_snapshot_values
      values = [1]
      source = Source.each(values)

      values << 2

      assert_equal [1, 2], source.run_with(Sink.to_a)
    end

    def test_each_does_not_close_original_enumerable
      enumerable = CloseableEnumerable.new([1])

      assert_equal [1], Source.each(enumerable).run_with(Sink.to_a)
      refute enumerable.closed?
    end

    def test_via_rejects_invalid_flow
      error = assert_raises(TypeError) do
        Source.each([1]).via(Object.new)
      end

      assert_match(/FiberStream::Flow/, error.message)
    end

    def test_run_with_rejects_invalid_sink
      error = assert_raises(TypeError) do
        Source.each([1]).run_with(Object.new)
      end

      assert_match(/FiberStream::Sink/, error.message)
    end

    def test_run_with_closes_flow_chain_when_sink_returns_early
      closed = false
      flow = Flow.__send__(:new) do |upstream|
        CloseTrackingStage.new(upstream) { closed = true }
      end
      sink = Sink.__send__(:new) { |stream| stream.next }

      assert_equal 1, Source.each([1, 2]).via(flow).run_with(sink)
      assert closed
    end

    def test_run_with_closes_flow_chain_when_sink_raises
      closed = false
      flow = Flow.__send__(:new) do |upstream|
        CloseTrackingStage.new(upstream) { closed = true }
      end
      sink = Sink.__send__(:new) { raise "sink boom" }

      error = assert_raises(RuntimeError) do
        Source.each([1]).via(flow).run_with(sink)
      end

      assert_equal "sink boom", error.message
      assert closed
    end

    def test_source_enumeration_exception_fails_stream
      error = assert_raises(RuntimeError) do
        Source.each(ExplodingEnumerable.new).run_with(Sink.to_a)
      end

      assert_equal "source boom", error.message
    end

    class CountingEnumerable
      attr_reader :each_calls

      def initialize(values)
        @values = values
        @each_calls = 0
      end

      def each(&block)
        @each_calls += 1
        @values.each(&block)
      end
    end

    class YieldingEnumerable
      include Enumerable

      def initialize(values)
        @values = values
      end

      def each
        @values.each { |value| yield value }
      end
    end

    class ExplodingEnumerable
      include Enumerable

      def each
        raise "source boom"
      end
    end

    class CloseableEnumerable
      include Enumerable

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

    class CloseTrackingStage
      def initialize(upstream, &on_close)
        @upstream = upstream
        @on_close = on_close
      end

      def next
        @upstream.next
      end

      def close
        @on_close.call
        @upstream.close
      end
    end
  end
end
