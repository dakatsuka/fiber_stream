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

    def test_constructor_is_private
      assert_raises(NoMethodError) do
        Source.new(-> { raise "unused" })
      end
    end

    def test_via_rejects_invalid_flow
      error = assert_raises(TypeError) do
        Source.each([1]).via(Object.new)
      end

      assert_match(/FiberStream::Flow/, error.message)
    end

    def test_map_convenience_delegates_to_flow_map
      result =
        Source.each([1, 2, 3])
          .map { |value| value * 2 }
          .run_with(Sink.to_a)

      assert_equal [2, 4, 6], result
    end

    def test_map_convenience_requires_block
      error = assert_raises(ArgumentError) do
        Source.each([1]).map
      end

      assert_match(/missing block/, error.message)
    end

    def test_select_convenience_delegates_to_flow_select
      result =
        Source.each([1, 2, 3, 4])
          .select(&:even?)
          .run_with(Sink.to_a)

      assert_equal [2, 4], result
    end

    def test_select_convenience_requires_block
      error = assert_raises(ArgumentError) do
        Source.each([1]).select
      end

      assert_match(/missing block/, error.message)
    end

    def test_take_convenience_delegates_to_flow_take
      result =
        Source.each([1, 2, 3])
          .take(2)
          .run_with(Sink.to_a)

      assert_equal [1, 2], result
    end

    def test_take_convenience_preserves_flow_validation
      error = assert_raises(ArgumentError) do
        Source.each([1]).take(-1)
      end

      assert_match(/count must be non-negative/, error.message)
    end

    def test_concat_appends_sources_in_order
      result =
        Source.each([1, 2])
          .concat(Source.each([3, 4]))
          .run_with(Sink.to_a)

      assert_equal [1, 2, 3, 4], result
    end

    def test_concat_rejects_invalid_source
      error = assert_raises(TypeError) do
        Source.each([1]).concat(Object.new)
      end

      assert_match(/FiberStream::Source/, error.message)
    end

    def test_concat_construction_is_lazy
      calls = []

      source =
        Source.each([1])
          .map do |value|
            calls << :left
            value
          end
          .concat(
            Source.each([2]).map do |value|
              calls << :right
              value
            end
          )

      assert_empty calls
      assert_equal [1, 2], source.run_with(Sink.to_a)
      assert_equal [:left, :right], calls
    end

    def test_concat_does_not_materialize_right_while_left_can_satisfy_demand
      right_materializations = 0
      right =
        build_materialization_tracking_source([2]) do
          right_materializations += 1
        end

      result =
        Source.each([1])
          .concat(right)
          .run_with(Sink.first)

      assert_equal 1, result
      assert_equal 0, right_materializations
    end

    def test_concat_materializes_right_when_empty_left_cannot_satisfy_demand
      right_materializations = 0
      right =
        build_materialization_tracking_source([2]) do
          right_materializations += 1
        end

      result =
        Source.each([])
          .concat(right)
          .run_with(Sink.first)

      assert_equal 2, result
      assert_equal 1, right_materializations
    end

    def test_concat_with_downstream_take_closes_left_without_materializing_right
      left_closed = false
      right_materializations = 0
      right =
        build_materialization_tracking_source([3]) do
          right_materializations += 1
        end
      flow = build_test_flow do |upstream|
        CloseTrackingStage.new(upstream) { left_closed = true }
      end

      result =
        Source.each([1, 2])
          .via(flow)
          .concat(right)
          .take(1)
          .run_with(Sink.to_a)

      assert_equal [1], result
      assert left_closed
      assert_equal 0, right_materializations
    end

    def test_concat_preserves_per_source_flows_and_applies_later_flows_to_both
      result =
        Source.each([1])
          .map { |value| value * 10 }
          .concat(Source.each([2]).map { |value| value * 100 })
          .map { |value| value + 1 }
          .run_with(Sink.to_a)

      assert_equal [11, 201], result
    end

    def test_concat_creates_fresh_materializations_for_each_run
      left = CountingEnumerable.new([1])
      right = CountingEnumerable.new([2])
      source = Source.each(left).concat(Source.each(right))

      assert_equal [1, 2], source.run_with(Sink.to_a)
      assert_equal [1, 2], source.run_with(Sink.to_a)
      assert_equal 2, left.each_calls
      assert_equal 2, right.each_calls
    end

    def test_concat_left_failure_prevents_right_materialization
      right_materializations = 0
      right =
        build_materialization_tracking_source([1]) do
          right_materializations += 1
        end

      error = assert_raises(RuntimeError) do
        Source.each(ExplodingEnumerable.new)
          .concat(right)
          .run_with(Sink.to_a)
      end

      assert_equal "source boom", error.message
      assert_equal 0, right_materializations
    end

    def test_concat_right_failure_propagates_after_left_completion
      error = assert_raises(RuntimeError) do
        Source.each([1])
          .concat(Source.each(ExplodingEnumerable.new))
          .run_with(Sink.to_a)
      end

      assert_equal "source boom", error.message
    end

    def test_concat_left_close_failure_during_transition_prevents_right_materialization
      right_materializations = 0
      right =
        build_materialization_tracking_source([2]) do
          right_materializations += 1
        end
      flow = build_test_flow do |upstream|
        CloseRaisingStage.new(upstream, "left close boom")
      end

      error = assert_raises(RuntimeError) do
        Source.each([])
          .via(flow)
          .concat(right)
          .run_with(Sink.first)
      end

      assert_equal "left close boom", error.message
      assert_equal 0, right_materializations
    end

    def test_concat_right_materialization_failure_propagates
      order = []
      flow = build_test_flow do |upstream|
        CloseTrackingStage.new(upstream) { order << :left_closed }
      end
      right =
        Source.__send__(
          :new,
          lambda do
            order << :right_materialized
            raise "right materialize boom"
          end
        )

      error = assert_raises(RuntimeError) do
        Source.each([])
          .via(flow)
          .concat(right)
          .run_with(Sink.first)
      end

      assert_equal "right materialize boom", error.message
      assert_equal [:left_closed, :right_materialized], order
    end

    def test_concat_right_close_failure_after_normal_completion_propagates
      flow = build_test_flow do |upstream|
        CloseRaisingStage.new(upstream, "right close boom")
      end

      error = assert_raises(RuntimeError) do
        Source.each([])
          .concat(Source.each([1]).via(flow))
          .run_with(Sink.to_a)
      end

      assert_equal "right close boom", error.message
    end

    def test_concat_close_failure_after_downstream_failure_is_suppressed
      flow = build_test_flow do |upstream|
        CloseRaisingStage.new(upstream, "right close boom")
      end
      sink = build_test_sink do |stream|
        stream.next
        raise "sink boom"
      end

      error = assert_raises(RuntimeError) do
        Source.each([])
          .concat(Source.each([1]).via(flow))
          .run_with(sink)
      end

      assert_equal "sink boom", error.message
    end

    def test_convenience_methods_compose_lazily
      called = false

      source =
        Source.each([1, 2, 3, 4])
          .map do |value|
            called = true
            value * 2
          end
          .select(&:even?)
          .take(2)

      refute called
      assert_equal [2, 4], source.run_with(Sink.to_a)
      assert called
    end

    def test_run_with_rejects_invalid_sink
      error = assert_raises(TypeError) do
        Source.each([1]).run_with(Object.new)
      end

      assert_match(/FiberStream::Sink/, error.message)
    end

    def test_run_with_closes_flow_chain_when_sink_returns_early
      closed = false
      flow = build_test_flow do |upstream|
        CloseTrackingStage.new(upstream) { closed = true }
      end

      assert_equal 1, Source.each([1, 2]).via(flow).run_with(Sink.first)
      assert closed
    end

    def test_run_with_closes_flow_chain_when_sink_raises
      closed = false
      flow = build_test_flow do |upstream|
        CloseTrackingStage.new(upstream) { closed = true }
      end
      sink = build_test_sink { raise "sink boom" }

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

    private

    def build_test_flow(&block)
      Flow.__send__(:new, &block)
    end

    def build_test_sink(&block)
      Sink.__send__(:new, &block)
    end

    def build_materialization_tracking_source(values, &on_materialize)
      Source.__send__(
        :new,
        lambda do
          on_materialize.call
          Source.each(values).__send__(:materialize)
        end
      )
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

    class CloseRaisingStage
      def initialize(upstream, message)
        @upstream = upstream
        @message = message
      end

      def next
        @upstream.next
      end

      def close
        @upstream.close
        raise @message
      end
    end
  end
end
