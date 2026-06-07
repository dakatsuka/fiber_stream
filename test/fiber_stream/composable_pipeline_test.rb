# frozen_string_literal: true

require_relative "../test_helper"

module FiberStream
  class ComposablePipelineTest < Minitest::Test
    def test_flow_via_composes_in_order
      flow =
        Flow.map { |value| value * 2 }
          .via(Flow.map(&:to_s))

      result =
        Source.each([1, 2, 3])
          .via(flow)
          .run_with(Sink.to_a)

      assert_equal ["2", "4", "6"], result
    end

    def test_flow_via_matches_repeated_source_via
      flow_a = Flow.map { |value| value + 1 }
      flow_b = Flow.select(&:even?)

      composed =
        Source.each([1, 2, 3, 4])
          .via(flow_a.via(flow_b))
          .run_with(Sink.to_a)

      repeated =
        Source.each([1, 2, 3, 4])
          .via(flow_a)
          .via(flow_b)
          .run_with(Sink.to_a)

      assert_equal repeated, composed
    end

    def test_flow_via_rejects_invalid_flow
      error = assert_raises(TypeError) do
        Flow.map { |value| value }.via(Object.new)
      end

      assert_match(/FiberStream::Flow/, error.message)
    end

    def test_flow_via_is_lazy
      called = false

      Flow.map do |value|
        called = true
        value
      end.via(Flow.map { |value| value })

      refute called
    end

    def test_composed_flow_is_reusable_without_sharing_pull_state
      flow =
        Flow.take(2)
          .via(Flow.map { |value| value * 10 })

      assert_equal [10, 20], Source.each([1, 2, 3]).via(flow).run_with(Sink.to_a)
      assert_equal [40, 50], Source.each([4, 5, 6]).via(flow).run_with(Sink.to_a)
    end

    def test_flow_via_closes_first_flow_when_second_attach_fails
      closed = false
      flow =
        build_close_tracking_flow { closed = true }
          .via(build_attach_raising_flow)

      error = assert_raises(RuntimeError) do
        Source.each([1]).via(flow).run_with(Sink.to_a)
      end

      assert_equal "attach boom", error.message
      assert closed
    end

    def test_flow_to_composes_sink
      sink =
        Flow.map { |value| value * 2 }
          .to(Sink.fold(0) { |sum, value| sum + value })

      assert_equal 12, Source.each([1, 2, 3]).run_with(sink)
    end

    def test_flow_to_rejects_invalid_sink
      error = assert_raises(TypeError) do
        Flow.map { |value| value }.to(Object.new)
      end

      assert_match(/FiberStream::Sink/, error.message)
    end

    def test_flow_to_is_lazy
      called = false

      Flow.map do |value|
        called = true
        value
      end.to(Sink.to_a)

      refute called
    end

    def test_composed_sink_closes_attached_flow_after_early_completion
      closed = false
      sink =
        build_close_tracking_flow { closed = true }
          .to(Sink.first)

      assert_equal 1, Source.each([1, 2, 3]).run_with(sink)
      assert closed
    end

    def test_composed_sink_closes_attached_flow_after_sink_failure
      closed = false
      sink =
        build_close_tracking_flow { closed = true }
          .to(build_test_sink { raise "sink boom" })

      error = assert_raises(RuntimeError) do
        Source.each([1]).run_with(sink)
      end

      assert_equal "sink boom", error.message
      assert closed
    end

    def test_composed_sink_prefers_sink_failure_over_close_failure
      sink =
        build_close_raising_flow
          .to(build_test_sink { raise "sink boom" })

      error = assert_raises(RuntimeError) do
        Source.each([1]).run_with(sink)
      end

      assert_equal "sink boom", error.message
    end

    def test_composed_sink_delivers_close_failure_after_success
      sink =
        build_close_raising_flow
          .to(Sink.to_a)

      error = assert_raises(RuntimeError) do
        Source.each([1]).run_with(sink)
      end

      assert_equal "close boom", error.message
    end

    def test_source_to_returns_runnable_pipeline
      pipeline =
        Source.each([1, 2, 3])
          .map { |value| value * 2 }
          .to(Sink.to_a)

      assert_instance_of Pipeline, pipeline
      assert_equal [2, 4, 6], pipeline.run
    end

    def test_source_to_rejects_invalid_sink
      error = assert_raises(TypeError) do
        Source.each([1]).to(Object.new)
      end

      assert_match(/FiberStream::Sink/, error.message)
    end

    def test_source_to_is_lazy
      called = false

      Source.each([1])
        .map do |value|
          called = true
          value
        end
        .to(Sink.to_a)

      refute called
    end

    def test_pipeline_run_matches_source_run_with
      source =
        Source.each([1, 2, 3])
          .select(&:odd?)

      assert_equal source.run_with(Sink.to_a), source.to(Sink.to_a).run
    end

    def test_pipeline_can_run_multiple_times_when_endpoints_are_repeatable
      pipeline =
        Source.each([1, 2])
          .to(Sink.to_a)

      assert_equal [1, 2], pipeline.run
      assert_equal [1, 2], pipeline.run
    end

    def test_pipeline_constructor_is_private
      assert_raises(NoMethodError) do
        Pipeline.new(Source.each([1]), Sink.to_a)
      end
    end

    private

    def build_test_sink(&block)
      Sink.build(&block)
    end

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

    def build_attach_raising_flow
      Flow.build do |_upstream|
        raise "attach boom"
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
