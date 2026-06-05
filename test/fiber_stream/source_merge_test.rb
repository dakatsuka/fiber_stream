# frozen_string_literal: true

require "async"
require_relative "../test_helper"

module FiberStream
  class SourceMergeTest < Minitest::Test
    def test_merge_emits_values_from_both_sources
      result =
        Sync do
          Source.each([[:left, 1], [:left, 2]])
            .merge(Source.each([[:right, 1], [:right, 2]]))
            .run_with(Sink.to_a)
        end

      assert_equal([[:left, 1], [:left, 2]], result.select { |side, _| side == :left })
      assert_equal([[:right, 1], [:right, 2]], result.select { |side, _| side == :right })
    end

    def test_merge_preserves_each_input_order
      result =
        Sync do
          Source.each((1..10).map { |value| [:left, value] })
            .merge(Source.each((1..10).map { |value| [:right, value] }))
            .run_with(Sink.to_a)
        end

      assert_equal (1..10).to_a, result.select { |side, _| side == :left }.map(&:last)
      assert_equal (1..10).to_a, result.select { |side, _| side == :right }.map(&:last)
    end

    def test_merge_continues_after_one_side_completes
      result =
        Sync do
          Source.each([1])
            .merge(Source.each([2, 3]))
            .run_with(Sink.to_a)
        end

      assert_equal [1, 2, 3], result.sort
    end

    def test_merge_applies_input_and_output_flows_in_scope
      result =
        Sync do
          Source.each([1, 2])
            .map { |value| value * 10 }
            .merge(Source.each([3]).map { |value| value * 100 })
            .map(&:to_s)
            .run_with(Sink.to_a)
        end

      assert_equal ["10", "20", "300"], result.sort
    end

    def test_merge_rejects_invalid_source
      error = assert_raises(TypeError) do
        Source.each([1]).merge(Object.new)
      end

      assert_match(/FiberStream::Source/, error.message)
    end

    def test_merge_is_lazy
      materializations = 0

      source =
        build_materialization_tracking_source([1]) { materializations += 1 }
          .merge(build_materialization_tracking_source([2]) { materializations += 1 })

      assert_equal 0, materializations

      result = Sync { source.run_with(Sink.to_a) }

      assert_equal [1, 2], result.sort
      assert_equal 2, materializations
    end

    def test_merge_does_not_require_scheduler_until_demand_reaches_merge
      materializations = 0

      result =
        build_materialization_tracking_source([1]) { materializations += 1 }
          .merge(build_materialization_tracking_source([2]) { materializations += 1 })
          .take(0)
          .run_with(Sink.to_a)

      assert_equal [], result
      assert_equal 0, materializations
    end

    def test_merge_requires_scheduler_when_demanded
      error = assert_raises(SchedulerRequiredError) do
        Source.each([1])
          .merge(Source.each([2]))
          .run_with(Sink.to_a)
      end

      assert_match(/Source\.merge requires Fiber\.scheduler/, error.message)
    end

    def test_merge_requires_non_blocking_fiber_when_demanded
      scheduler = Async::Scheduler.new
      Fiber.set_scheduler(scheduler)

      error = assert_raises(SchedulerRequiredError) do
        Source.each([1])
          .merge(Source.each([2]))
          .run_with(Sink.to_a)
      end

      assert_match(/Source\.merge requires a non-blocking fiber/, error.message)
    ensure
      Fiber.set_scheduler(nil)
      scheduler&.close
    end

    def test_merge_closes_both_sources_after_early_completion
      left_closed = false
      right_closed = false

      result =
        Sync do
          build_close_tracking_source([1, 2, 3]) { left_closed = true }
            .merge(build_close_tracking_source([4, 5, 6]) { right_closed = true })
            .run_with(Sink.first)
        end

      refute_nil result
      assert left_closed
      assert right_closed
    end

    def test_merge_downstream_failure_closes_sources_and_remains_primary
      left_closed = false
      right_closed = false
      sink =
        Sink.__send__(:new) do |stream|
          stream.next
          raise "sink boom"
        end

      error = assert_raises(RuntimeError) do
        Sync do
          build_close_tracking_source([1, 2, 3]) { left_closed = true }
            .merge(build_close_tracking_source([4, 5, 6]) { right_closed = true })
            .run_with(sink)
        end
      end

      assert_equal "sink boom", error.message
      assert left_closed
      assert right_closed
    end

    def test_merge_close_failure_after_early_completion_propagates
      right_closed = false

      error = assert_raises(RuntimeError) do
        Sync do
          build_close_raising_source([1, 2, 3])
            .merge(build_close_tracking_source([4, 5, 6]) { right_closed = true })
            .run_with(Sink.first)
        end
      end

      assert_equal "close boom", error.message
      assert right_closed
    end

    def test_merge_both_close_failures_after_early_completion_prefers_receiver_close
      error = assert_raises(RuntimeError) do
        Sync do
          build_close_raising_source([1, 2, 3], message: "left close boom")
            .merge(build_close_raising_source([4, 5, 6], message: "right close boom"))
            .run_with(Sink.first)
        end
      end

      assert_equal "left close boom", error.message
    end

    def test_merge_propagates_input_failure_and_closes_other_side
      other_closed = false

      error = assert_raises(RuntimeError) do
        Sync do
          build_next_raising_source(raise_on_call: 1)
            .merge(build_close_tracking_source([1, 2, 3]) { other_closed = true })
            .run_with(Sink.to_a)
        end
      end

      assert_equal "next boom", error.message
      assert other_closed
    end

    def test_merge_input_failure_wins_over_same_side_close_failure
      error = assert_raises(RuntimeError) do
        Sync do
          build_next_raising_source(raise_on_call: 1, close_error: true)
            .merge(Source.each([1]))
            .run_with(Sink.to_a)
        end
      end

      assert_equal "next boom", error.message
    end

    def test_merge_suppresses_queued_upstream_error_after_early_completion
      result =
        Sync do
          Source.each([1, 2])
            .map { |value| value == 2 ? raise("queued boom") : value }
            .merge(Source.each([]))
            .run_with(Sink.first)
        end

      assert_equal 1, result
    end

    def test_merge_materialization_failure_closes_other_materialized_side
      other_closed = false

      error = assert_raises(RuntimeError) do
        Sync do
          build_materialization_raising_source("materialize boom", delay: 0.01)
            .merge(build_close_tracking_source([1, 2, 3]) { other_closed = true })
            .run_with(Sink.to_a)
        end
      end

      assert_equal "materialize boom", error.message
      assert other_closed
    end

    def test_merge_propagates_producer_close_failure_after_normal_completion
      error = assert_raises(RuntimeError) do
        Sync do
          build_close_raising_source([1])
            .merge(Source.each([]))
            .run_with(Sink.to_a)
        end
      end

      assert_equal "close boom", error.message
    end

    def test_merge_repeated_pulls_after_completion_do_not_restart_producers
      materializations = 0
      sink =
        Sink.__send__(:new) do |stream|
          first = stream.next
          second = stream.next
          third = stream.next
          [first, second, third]
        end

      result =
        Sync do
          build_materialization_tracking_source([1]) { materializations += 1 }
            .merge(build_materialization_tracking_source([]) { materializations += 1 })
            .run_with(sink)
        end

      assert_equal 1, result.fetch(0)
      assert Pull.done?(result.fetch(1))
      assert Pull.done?(result.fetch(2))
      assert_equal 2, materializations
    end

    def test_merge_self_materializes_both_sides_independently
      source = Source.each([1, 2])

      result =
        Sync do
          source.merge(source).run_with(Sink.to_a)
        end

      assert_equal [1, 1, 2, 2], result.sort
    end

    def test_merge_bounds_producer_run_ahead
      pulled = 0

      Sync do
        Source.each(1.upto(100).to_a)
          .via(build_next_counting_flow { pulled += 1 })
          .merge(
            Source.each(101.upto(200).to_a)
              .via(build_next_counting_flow { pulled += 1 })
          )
          .run_with(Sink.first)

        sleep 0
      end

      assert_operator pulled, :<=, 5
    end

    def test_merge_mailbox_wait_does_not_block_async_reactor
      reactor_progressed = false

      result =
        Sync do |task|
          task.async do
            sleep 0.01
            reactor_progressed = true
          end

          Source.each([1])
            .map do |value|
              sleep 0.02
              value
            end
            .merge(Source.each([]))
            .run_with(Sink.first)
        end

      assert_equal 1, result
      assert reactor_progressed
    end

    def test_merge_closes_producer_blocked_on_enqueue_without_closed_boundary_error
      result =
        Sync do
          Source.each(1.upto(100).to_a)
            .merge(Source.each(101.upto(200).to_a))
            .run_with(Sink.first)
        end

      refute_nil result
    end

    private

    def build_materialization_tracking_source(values, &on_materialize)
      Source.__send__(:new, lambda {
        on_materialize.call
        Pull.each(values)
      })
    end

    def build_materialization_raising_source(message, delay: 0)
      Source.__send__(:new, lambda {
        sleep delay if delay.positive?
        raise message
      })
    end

    def build_close_tracking_source(values, &on_close)
      Source.each(values).via(build_close_tracking_flow(&on_close))
    end

    def build_close_raising_source(values, message: "close boom")
      Source.each(values).via(build_close_raising_flow(message: message))
    end

    def build_next_raising_source(raise_on_call:, close_error: false)
      Source.each([1, 2, 3])
        .via(build_next_raising_flow(raise_on_call: raise_on_call))
        .via(close_error ? build_close_raising_flow : Flow.map { |value| value })
    end

    def build_close_tracking_flow(&on_close)
      Flow.__send__(:new) do |upstream|
        CloseTrackingStage.new(upstream, &on_close)
      end
    end

    def build_close_raising_flow(message: "close boom")
      Flow.__send__(:new) do |upstream|
        CloseRaisingStage.new(upstream, message)
      end
    end

    def build_next_counting_flow(&on_next)
      Flow.__send__(:new) do |upstream|
        NextCountingStage.new(upstream, &on_next)
      end
    end

    def build_next_raising_flow(raise_on_call:)
      Flow.__send__(:new) do |upstream|
        NextRaisingStage.new(upstream, raise_on_call)
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
      def initialize(upstream, message)
        @upstream = upstream
        @message = message
        @closed = false
      end

      def next
        @upstream.next
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
        raise @message
      end
    end

    class NextCountingStage
      def initialize(upstream, &on_next)
        @upstream = upstream
        @on_next = on_next
      end

      def next
        @on_next.call
        @upstream.next
      end

      def close
        @upstream.close
      end
    end

    class NextRaisingStage
      def initialize(upstream, raise_on_call)
        @upstream = upstream
        @raise_on_call = raise_on_call
        @calls = 0
      end

      def next
        @calls += 1
        raise "next boom" if @calls == @raise_on_call

        @upstream.next
      end

      def close
        @upstream.close
      end
    end
  end
end
