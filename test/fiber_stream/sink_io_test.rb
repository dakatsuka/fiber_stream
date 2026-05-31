# frozen_string_literal: true

require "async"
require_relative "../test_helper"

module FiberStream
  class SinkIOTest < Minitest::Test
    def test_io_rejects_object_without_write
      error = assert_raises(TypeError) do
        Sink.io(Object.new)
      end

      assert_match(/write/, error.message)
    end

    def test_io_rejects_owned_object_without_close
      io = WriteOnlyIO.new

      error = assert_raises(TypeError) do
        Sink.io(io, close: true)
      end

      assert_match(/close/, error.message)
    end

    def test_io_rejects_flush_object_without_flush
      io = WriteOnlyIO.new

      error = assert_raises(TypeError) do
        Sink.io(io, flush: true)
      end

      assert_match(/flush/, error.message)
    end

    def test_io_rejects_non_boolean_close
      error = assert_raises(TypeError) do
        Sink.io(WritableIO.new, close: nil)
      end

      assert_match(/close must be true or false/, error.message)
    end

    def test_io_rejects_non_boolean_flush
      error = assert_raises(TypeError) do
        Sink.io(WritableIO.new, flush: nil)
      end

      assert_match(/flush must be true or false/, error.message)
    end

    def test_io_is_lazy
      io = WritableIO.new

      Sink.io(io, close: true, flush: true)

      assert_empty io.writes
      assert_equal 0, io.flush_calls
      refute io.closed?
    end

    def test_io_requires_scheduler_when_writing
      io = WritableIO.new

      error = assert_raises(SchedulerRequiredError) do
        Source.each(["chunk"]).run_with(Sink.io(io))
      end

      assert_match(/Fiber.scheduler/, error.message)
      assert_empty io.writes
      refute io.closed?
    end

    def test_io_closes_owned_io_when_scheduler_is_missing
      io = WritableIO.new

      assert_raises(SchedulerRequiredError) do
        Source.each(["chunk"]).run_with(Sink.io(io, close: true))
      end

      assert_empty io.writes
      assert io.closed?
    end

    def test_io_preserves_scheduler_error_when_close_also_fails
      io = CloseRaisingIO.new

      error = assert_raises(SchedulerRequiredError) do
        Source.each(["chunk"]).run_with(Sink.io(io, close: true))
      end

      assert_match(/Fiber.scheduler/, error.message)
      assert io.closed?
    end

    def test_io_requires_non_blocking_fiber_when_writing
      io = WritableIO.new
      result = nil
      error = nil

      Sync do
        Fiber.new(blocking: true) do
          begin
            result = Source.each(["chunk"]).run_with(Sink.io(io))
          rescue SchedulerRequiredError => exception
            error = exception
          end
        end.resume
      end

      assert_nil result
      assert_instance_of SchedulerRequiredError, error
      assert_match(/non-blocking fiber/, error.message)
      assert_empty io.writes
    end

    def test_io_requires_non_blocking_fiber_when_flushing
      io = WritableIO.new
      result = nil
      error = nil

      Sync do
        Fiber.new(blocking: true) do
          begin
            result = Source.each([]).run_with(Sink.io(io, flush: true))
          rescue SchedulerRequiredError => exception
            error = exception
          end
        end.resume
      end

      assert_nil result
      assert_instance_of SchedulerRequiredError, error
      assert_match(/non-blocking fiber/, error.message)
      assert_equal 0, io.flush_calls
    end

    def test_io_requires_non_blocking_fiber_when_closing_after_normal_completion
      io = WritableIO.new
      result = nil
      error = nil

      Sync do
        Fiber.new(blocking: true) do
          begin
            result = Source.each([]).run_with(Sink.io(io, close: true))
          rescue SchedulerRequiredError => exception
            error = exception
          end
        end.resume
      end

      assert_nil result
      assert_instance_of SchedulerRequiredError, error
      assert_match(/non-blocking fiber/, error.message)
      assert io.closed?
    end

    def test_io_writes_chunks_and_returns_write_count
      io = WritableIO.new

      result =
        Sync do
          Source.each(["a", "", "b"]).run_with(Sink.io(io))
        end

      assert_equal 3, result
      assert_equal ["a", "", "b"], io.writes
    end

    def test_io_writes_to_ruby_core_pipe
      reader, writer = IO.pipe

      result =
        Sync do
          Source.each(["hello", "world"])
            .run_with(Sink.io(writer, close: true))
        end

      assert_equal 2, result
      assert_equal "helloworld", reader.read
    ensure
      reader&.close
    end

    def test_io_pulls_one_upstream_element_per_write_attempt
      io = WriteRaisingOnValueIO.new("b")
      pulled = 0

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each(["a", "b", "c"])
            .map { |value| pulled += 1; value }
            .run_with(Sink.io(io))
        end
      end

      assert_equal "write boom", error.message
      assert_equal 2, pulled
      assert_equal ["a"], io.writes
      assert_equal 2, io.write_calls
    end

    def test_io_rejects_non_string_elements
      io = WritableIO.new

      error = assert_raises(TypeError) do
        Sync do
          Source.each(["a", :not_a_string])
            .run_with(Sink.io(io, close: true))
        end
      end

      assert_match(/Sink.io elements must be String/, error.message)
      assert_equal ["a"], io.writes
      assert io.closed?
    end

    def test_io_does_not_flush_by_default
      io = WritableIO.new

      Sync do
        Source.each(["a"]).run_with(Sink.io(io))
      end

      assert_equal 0, io.flush_calls
    end

    def test_io_flushes_once_on_normal_completion
      io = WritableIO.new

      result =
        Sync do
          Source.each(["a"]).run_with(Sink.io(io, flush: true))
        end

      assert_equal 1, result
      assert_equal 1, io.flush_calls
    end

    def test_io_does_not_flush_after_write_failure
      io = WriteRaisingOnValueIO.new("b")

      assert_raises(RuntimeError) do
        Sync do
          Source.each(["a", "b"]).run_with(Sink.io(io, flush: true))
        end
      end

      assert_equal 0, io.flush_calls
    end

    def test_io_does_not_flush_after_upstream_failure
      io = WritableIO.new

      assert_raises(RuntimeError) do
        Sync do
          Source.each(["a", "b"])
            .map { |value| raise "upstream boom" if value == "b"; value }
            .run_with(Sink.io(io, flush: true))
        end
      end

      assert_equal 0, io.flush_calls
    end

    def test_io_does_not_flush_after_type_failure
      io = WritableIO.new

      assert_raises(TypeError) do
        Sync do
          Source.each([:not_a_string]).run_with(Sink.io(io, flush: true))
        end
      end

      assert_equal 0, io.flush_calls
    end

    def test_io_does_not_flush_after_scheduler_failure
      io = WritableIO.new

      assert_raises(SchedulerRequiredError) do
        Source.each(["a"]).run_with(Sink.io(io, flush: true))
      end

      assert_equal 0, io.flush_calls
    end

    def test_empty_upstream_without_flush_or_close_does_not_require_scheduler
      io = WritableIO.new

      result = Source.each([]).run_with(Sink.io(io))

      assert_equal 0, result
      assert_empty io.writes
      assert_equal 0, io.flush_calls
      refute io.closed?
    end

    def test_empty_upstream_with_flush_requires_scheduler
      io = WritableIO.new

      error = assert_raises(SchedulerRequiredError) do
        Source.each([]).run_with(Sink.io(io, flush: true))
      end

      assert_match(/Fiber.scheduler/, error.message)
      assert_equal 0, io.flush_calls
    end

    def test_empty_upstream_with_close_requires_scheduler_and_closes
      io = WritableIO.new

      assert_raises(SchedulerRequiredError) do
        Source.each([]).run_with(Sink.io(io, close: true))
      end

      assert io.closed?
    end

    def test_io_does_not_close_unowned_io_on_normal_completion
      io = WritableIO.new

      Sync do
        Source.each(["a"]).run_with(Sink.io(io))
      end

      refute io.closed?
    end

    def test_io_does_not_close_unowned_io_on_upstream_failure
      io = WritableIO.new

      assert_raises(RuntimeError) do
        Sync do
          Source.each(["a", "b"])
            .map { |value| raise "upstream boom" if value == "b"; value }
            .run_with(Sink.io(io))
        end
      end

      refute io.closed?
    end

    def test_io_does_not_close_unowned_io_on_type_failure
      io = WritableIO.new

      assert_raises(TypeError) do
        Sync do
          Source.each([:not_a_string]).run_with(Sink.io(io))
        end
      end

      refute io.closed?
    end

    def test_io_does_not_close_unowned_io_on_write_failure
      io = WriteRaisingOnValueIO.new("a")

      assert_raises(RuntimeError) do
        Sync do
          Source.each(["a"]).run_with(Sink.io(io))
        end
      end

      refute io.closed?
    end

    def test_io_closes_owned_io_on_normal_completion
      io = WritableIO.new

      Sync do
        Source.each(["a"]).run_with(Sink.io(io, close: true))
      end

      assert io.closed?
    end

    def test_io_closes_owned_io_on_upstream_failure
      io = WritableIO.new

      assert_raises(RuntimeError) do
        Sync do
          Source.each(["a", "b"])
            .map { |value| raise "upstream boom" if value == "b"; value }
            .run_with(Sink.io(io, close: true))
        end
      end

      assert io.closed?
    end

    def test_io_closes_owned_io_on_write_failure
      io = WriteRaisingOnValueIO.new("a")

      assert_raises(RuntimeError) do
        Sync do
          Source.each(["a"]).run_with(Sink.io(io, close: true))
        end
      end

      assert io.closed?
    end

    def test_io_delivers_close_failure_after_normal_completion
      io = CloseRaisingIO.new

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each(["a"]).run_with(Sink.io(io, close: true))
        end
      end

      assert_equal "close boom", error.message
    end

    def test_io_delivers_flush_failure_after_normal_completion
      io = FlushRaisingIO.new

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each(["a"]).run_with(Sink.io(io, flush: true))
        end
      end

      assert_equal "flush boom", error.message
    end

    def test_io_prefers_flush_failure_over_close_failure
      io = FlushAndCloseRaisingIO.new

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each(["a"]).run_with(Sink.io(io, flush: true, close: true))
        end
      end

      assert_equal "flush boom", error.message
      assert io.closed?
    end

    def test_io_prefers_upstream_failure_over_close_failure
      io = CloseRaisingIO.new

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each(["a", "b"])
            .map { |value| raise "upstream boom" if value == "b"; value }
            .run_with(Sink.io(io, close: true))
        end
      end

      assert_equal "upstream boom", error.message
      assert io.closed?
    end

    def test_io_prefers_type_failure_over_close_failure
      io = CloseRaisingIO.new

      error = assert_raises(TypeError) do
        Sync do
          Source.each([:not_a_string]).run_with(Sink.io(io, close: true))
        end
      end

      assert_match(/Sink.io elements must be String/, error.message)
      assert io.closed?
    end

    def test_io_prefers_write_failure_over_close_failure
      io = WriteAndCloseRaisingIO.new("a")

      error = assert_raises(RuntimeError) do
        Sync do
          Source.each(["a"]).run_with(Sink.io(io, close: true))
        end
      end

      assert_equal "write boom", error.message
      assert io.closed?
    end

    def test_io_repeated_materialization_writes_same_io_state
      io = WritableIO.new
      sink = Sink.io(io)

      first =
        Sync do
          Source.each(["a"]).run_with(sink)
        end
      second =
        Sync do
          Source.each(["b", "c"]).run_with(sink)
        end

      assert_equal 1, first
      assert_equal 2, second
      assert_equal ["a", "b", "c"], io.writes
    end

    class WriteOnlyIO
      def write(_chunk)
        true
      end
    end

    class WritableIO < WriteOnlyIO
      attr_reader :flush_calls, :write_calls, :writes

      def initialize
        super()
        @closed = false
        @flush_calls = 0
        @write_calls = 0
        @writes = []
      end

      def write(chunk)
        @write_calls += 1
        @writes << chunk
        chunk.bytesize
      end

      def flush
        @flush_calls += 1
      end

      def close
        @closed = true
      end

      def closed?
        @closed
      end
    end

    class WriteRaisingOnValueIO < WritableIO
      def initialize(raising_value)
        super()
        @raising_value = raising_value
      end

      def write(chunk)
        @write_calls += 1
        raise "write boom" if chunk == @raising_value

        @writes << chunk
        chunk.bytesize
      end
    end

    class CloseRaisingIO < WritableIO
      def close
        super
        raise "close boom"
      end
    end

    class FlushRaisingIO < WritableIO
      def flush
        super
        raise "flush boom"
      end
    end

    class FlushAndCloseRaisingIO < FlushRaisingIO
      def close
        @closed = true
        raise "close boom"
      end
    end

    class WriteAndCloseRaisingIO < WriteRaisingOnValueIO
      def close
        @closed = true
        raise "close boom"
      end
    end
  end
end
