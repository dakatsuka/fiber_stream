# frozen_string_literal: true

require "async"
require "async/scheduler"
require_relative "../test_helper"

module FiberStream
  class PipelineBackgroundTest < Minitest::Test
    def test_run_async_requires_scheduler
      error = assert_raises(SchedulerRequiredError) do
        Source.each([1])
          .to(Sink.to_a)
          .run_async
      end

      assert_match(/Fiber.scheduler/, error.message)
    end

    def test_run_async_requires_non_blocking_fiber
      scheduler = Async::Scheduler.new
      Fiber.set_scheduler(scheduler)

      error = assert_raises(SchedulerRequiredError) do
        Source.each([1])
          .to(Sink.to_a)
          .run_async
      end

      assert_match(/non-blocking fiber/, error.message)
    ensure
      Fiber.set_scheduler(nil)
      scheduler&.close
    end

    def test_wait_returns_materialized_value
      result =
        Sync do
          running =
            Source.each([1, 2, 3])
              .map { |value| value * 2 }
              .to(Sink.to_a)
              .run_async

          running.wait
        end

      assert_equal [2, 4, 6], result
    end

    def test_wait_reraises_stream_failure
      error = assert_raises(RuntimeError) do
        Sync do
          running =
            Source.each([1])
              .map { explode_background }
              .to(Sink.to_a)
              .run_async

          running.wait
        end
      end

      assert_equal "background boom", error.message
    end

    def test_wait_reraises_non_standard_stream_failure
      error = assert_raises(NotImplementedError) do
        Sync do
          running =
            Source.each([1])
              .map { explode_not_implemented }
              .to(Sink.to_a)
              .run_async

          running.wait
        end
      end

      assert_equal "not implemented boom", error.message
    end

    def test_system_exit_is_not_swallowed_by_background_fiber
      running = nil

      error = assert_raises(SystemExit) do
        Sync do
          running =
            RunningPipeline.start(Fiber.scheduler) do
              sleep 0
              raise SystemExit, "exit boom"
            end

          2.times { sleep 0 }
        end
      end

      assert_equal "exit boom", error.message
      assert running.done?

      wait_error = assert_raises(SystemExit) { running.wait }
      assert_same error, wait_error
    end

    def test_signal_exception_is_not_swallowed_by_background_fiber
      running = nil

      error = assert_raises(Interrupt) do
        Sync do
          running =
            RunningPipeline.start(Fiber.scheduler) do
              sleep 0
              raise Interrupt, "interrupt boom"
            end

          2.times { sleep 0 }
        end
      end

      assert_equal "interrupt boom", error.message
      assert running.done?

      wait_error = assert_raises(Interrupt) { running.wait }
      assert_same error, wait_error
    end

    def test_wait_is_replayable_after_success
      Sync do
        running =
          Source.each([1])
            .to(Sink.to_a)
            .run_async

        assert_equal [1], running.wait
        assert running.done?
        assert_equal [1], running.wait
      end
    end

    def test_wait_is_replayable_after_failure
      Sync do
        running =
          Source.each([1])
            .map { explode_repeat }
            .to(Sink.to_a)
            .run_async

        first_error = assert_raises(RuntimeError) { running.wait }
        second_error = assert_raises(RuntimeError) { running.wait }

        assert_equal "repeat boom", first_error.message
        assert_same first_error, second_error
      end
    end

    def test_concurrent_waiters_receive_same_completion
      Sync do |task|
        running =
          Source.each([1])
            .map { |value| sleep 0.01; value }
            .to(Sink.to_a)
            .run_async

        waiter_a = task.async { running.wait }
        waiter_b = task.async { running.wait }

        assert_equal [1], waiter_a.wait
        assert_equal [1], waiter_b.wait
        assert running.done?
      end
    end

    def test_run_async_creates_one_materialization_per_call
      enumerable = CountingEnumerable.new([1])

      Sync do
        pipeline = Source.each(enumerable).to(Sink.to_a)

        assert_equal [1], pipeline.run_async.wait
        assert_equal [1], pipeline.run_async.wait
      end

      assert_equal 2, enumerable.each_calls
    end

    def test_cancel_interrupts_running_pipeline_and_closes_upstream
      closed = false

      error = assert_raises(PipelineCancelledError) do
        Sync do
          running =
            Source.each([1])
              .via(build_close_tracking_flow { closed = true })
              .map { |value| sleep 60; value }
              .to(Sink.to_a)
              .run_async

          sleep 0
          running.cancel
          running.wait
        end
      end

      assert_equal "pipeline cancelled", error.message
      assert closed
    end

    def test_cancel_is_idempotent_and_records_request_state
      Sync do
        running =
          Source.each([1])
            .map { |value| sleep 60; value }
            .to(Sink.to_a)
            .run_async

        sleep 0
        refute running.cancel_requested?

        assert_same running, running.cancel
        assert running.cancel_requested?
        assert_same running, running.cancel

        assert_raises(PipelineCancelledError) { running.wait }
      end
    end

    def test_cancel_after_completion_has_no_effect
      Sync do
        running =
          Source.each([1])
            .to(Sink.to_a)
            .run_async

        assert_equal [1], running.wait
        assert running.done?

        running.cancel

        refute running.cancel_requested?
        assert_equal [1], running.wait
      end
    end

    def test_failed_cancel_does_not_record_request_state
      Sync do
        running =
          RunningPipeline.start(Object.new) do
            sleep 0.01
            :finished
          end

        sleep 0

        error = assert_raises(NotImplementedError) { running.cancel }

        assert_match(/fiber_interrupt/, error.message)
        refute running.cancel_requested?
        assert_equal :finished, running.wait
      end
    end

    def test_cancel_interrupt_failure_does_not_record_request_state
      Sync do
        running =
          RunningPipeline.start(InterruptRaisingScheduler.new) do
            sleep 0.01
            :finished
          end

        sleep 0

        error = assert_raises(NotImplementedError) { running.cancel }

        assert_match(/interrupt unavailable/, error.message)
        refute running.cancel_requested?
        assert_equal :finished, running.wait
      end
    end

    def test_user_raised_pipeline_cancelled_error_is_stream_failure
      error = assert_raises(PipelineCancelledError) do
        Sync do
          running =
            Source.each([1])
              .map { explode_with_user_cancelled_error }
              .to(Sink.to_a)
              .run_async

          running.wait
        end
      end

      assert_equal "user cancellation value", error.message
    end

    private

    def explode_background
      raise "background boom"
    end

    def explode_repeat
      raise "repeat boom"
    end

    def explode_not_implemented
      raise NotImplementedError, "not implemented boom"
    end

    def explode_with_user_cancelled_error
      raise PipelineCancelledError, "user cancellation value"
    end

    def build_close_tracking_flow(&on_close)
      Flow.build do |upstream|
        CloseTrackingStage.new(upstream, &on_close)
      end
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

    class InterruptRaisingScheduler
      def fiber_interrupt(_fiber, _exception)
        raise NotImplementedError, "interrupt unavailable"
      end
    end
  end
end
