# frozen_string_literal: true

require "async"
require_relative "../test_helper"

module FiberStream
  class SourceIOTest < Minitest::Test
    def test_io_rejects_object_without_readpartial
      error = assert_raises(TypeError) do
        Source.io(Object.new)
      end

      assert_match(/readpartial/, error.message)
    end

    def test_io_rejects_owned_object_without_close
      io = ReadOnlyIO.new(["chunk"])

      error = assert_raises(TypeError) do
        Source.io(io, close: true)
      end

      assert_match(/close/, error.message)
    end

    def test_io_rejects_non_integer_chunk_size
      error = assert_raises(TypeError) do
        Source.io(ReadOnlyIO.new(["chunk"]), chunk_size: 1.5)
      end

      assert_match(/chunk_size must be an Integer/, error.message)
    end

    def test_io_rejects_non_positive_chunk_size
      error = assert_raises(ArgumentError) do
        Source.io(ReadOnlyIO.new(["chunk"]), chunk_size: 0)
      end

      assert_match(/chunk_size must be positive/, error.message)
    end

    def test_io_rejects_non_boolean_close
      error = assert_raises(TypeError) do
        Source.io(ReadOnlyIO.new(["chunk"]), close: nil)
      end

      assert_match(/close must be true or false/, error.message)
    end

    def test_io_is_lazy
      io = CloseableIO.new(["chunk"])

      Source.io(io, close: true)

      assert_equal 0, io.read_calls
      refute io.closed?
    end

    def test_io_requires_scheduler_when_demanded
      io = CloseableIO.new(["chunk"])

      error = assert_raises(SchedulerRequiredError) do
        Source.io(io).run_with(Sink.to_a)
      end

      assert_match(/Fiber.scheduler/, error.message)
      assert_equal 0, io.read_calls
      refute io.closed?
    end

    def test_io_closes_owned_io_when_scheduler_is_missing
      io = CloseableIO.new(["chunk"])

      assert_raises(SchedulerRequiredError) do
        Source.io(io, close: true).run_with(Sink.to_a)
      end

      assert_equal 0, io.read_calls
      assert io.closed?
    end

    def test_io_preserves_scheduler_error_when_close_also_fails
      io = CloseRaisingIO.new(["chunk"])

      error = assert_raises(SchedulerRequiredError) do
        Source.io(io, close: true).run_with(Sink.to_a)
      end

      assert_match(/Fiber.scheduler/, error.message)
      assert io.closed?
    end

    def test_io_requires_non_blocking_fiber_when_demanded
      io = CloseableIO.new(["chunk"])
      result = nil
      error = nil

      Sync do
        Fiber.new(blocking: true) do
          begin
            result = Source.io(io).run_with(Sink.to_a)
          rescue SchedulerRequiredError => exception
            error = exception
          end
        end.resume
      end

      assert_nil result
      assert_instance_of SchedulerRequiredError, error
      assert_match(/non-blocking fiber/, error.message)
      assert_equal 0, io.read_calls
    end

    def test_io_emits_chunks_and_completes_on_eof
      reader, writer = IO.pipe
      writer.write("hello")
      writer.close

      result =
        Sync do
          Source.io(reader, chunk_size: 2, close: true).run_with(Sink.to_a)
        end

      assert_equal ["he", "ll", "o"], result
      assert reader.closed?
    end

    def test_io_reads_at_most_once_per_pull
      io = CloseableIO.new(["a", "b", "c"])

      result =
        Sync do
          Source.io(io).run_with(Sink.first)
        end

      assert_equal "a", result
      assert_equal 1, io.read_calls
    end

    def test_io_does_not_close_unowned_io_on_eof
      io = CloseableIO.new(["a"])

      result =
        Sync do
          Source.io(io).run_with(Sink.to_a)
        end

      assert_equal ["a"], result
      refute io.closed?
    end

    def test_io_does_not_close_unowned_io_after_early_completion
      io = CloseableIO.new(["a", "b"])

      result =
        Sync do
          Source.io(io).run_with(Sink.first)
        end

      assert_equal "a", result
      refute io.closed?
    end

    def test_io_does_not_close_unowned_io_when_downstream_fails
      io = CloseableIO.new(["a"])
      sink =
        Sink.build do |stream|
          stream.next
          raise "sink boom"
        end

      error = assert_raises(RuntimeError) do
        Sync do
          Source.io(io).run_with(sink)
        end
      end

      assert_equal "sink boom", error.message
      refute io.closed?
    end

    def test_io_closes_owned_io_on_eof
      io = CloseableIO.new(["a"])

      result =
        Sync do
          Source.io(io, close: true).run_with(Sink.to_a)
        end

      assert_equal ["a"], result
      assert io.closed?
    end

    def test_io_closes_owned_io_after_early_completion
      io = CloseableIO.new(["a", "b"])

      result =
        Sync do
          Source.io(io, close: true).run_with(Sink.first)
        end

      assert_equal "a", result
      assert io.closed?
    end

    def test_io_closes_owned_io_when_downstream_fails
      io = CloseableIO.new(["a"])
      sink =
        Sink.build do |stream|
          stream.next
          raise "sink boom"
        end

      error = assert_raises(RuntimeError) do
        Sync do
          Source.io(io, close: true).run_with(sink)
        end
      end

      assert_equal "sink boom", error.message
      assert io.closed?
    end

    def test_io_propagates_close_failure_after_eof
      io = CloseRaisingIO.new(["a"])

      error = assert_raises(RuntimeError) do
        Sync do
          Source.io(io, close: true).run_with(Sink.to_a)
        end
      end

      assert_equal "close boom", error.message
    end

    def test_io_prefers_read_failure_over_close_failure
      io = CloseRaisingIO.new([RuntimeError.new("read boom")])

      error = assert_raises(RuntimeError) do
        Sync do
          Source.io(io, close: true).run_with(Sink.to_a)
        end
      end

      assert_equal "read boom", error.message
      assert io.closed?
    end

    def test_io_delivers_close_failure_after_early_completion
      io = CloseRaisingIO.new(["a", "b"])

      error = assert_raises(RuntimeError) do
        Sync do
          Source.io(io, close: true).run_with(Sink.first)
        end
      end

      assert_equal "close boom", error.message
    end

    def test_io_prefers_downstream_failure_over_close_failure
      io = CloseRaisingIO.new(["a"])
      sink =
        Sink.build do |stream|
          stream.next
          raise "sink boom"
        end

      error = assert_raises(RuntimeError) do
        Sync do
          Source.io(io, close: true).run_with(sink)
        end
      end

      assert_equal "sink boom", error.message
      assert io.closed?
    end

    def test_io_rejects_non_string_readpartial_result
      io = CloseableIO.new([:not_a_string])

      error = assert_raises(TypeError) do
        Sync do
          Source.io(io, close: true).run_with(Sink.to_a)
        end
      end

      assert_match(/readpartial must return a String/, error.message)
      assert io.closed?
    end

    def test_io_repeated_pulls_after_eof_do_not_read_again
      io = CloseableIO.new(["a"])
      sink = build_repeated_pull_sink(4)

      result =
        Sync do
          Source.io(io).run_with(sink)
        end

      assert_equal "a", result.fetch(0)
      assert_equal 2, io.read_calls
    end

    def test_io_repeated_materialization_reads_same_io_state
      io = CloseableIO.new(["a", "b"])
      source = Source.io(io)

      first =
        Sync do
          source.run_with(Sink.first)
        end
      second =
        Sync do
          source.run_with(Sink.to_a)
        end

      assert_equal "a", first
      assert_equal ["b"], second
    end

    private

    def build_repeated_pull_sink(count)
      Sink.build do |stream|
        count.times.map { stream.next }
      end
    end

    class ReadOnlyIO
      def initialize(chunks)
        @chunks = chunks
      end

      def readpartial(_chunk_size)
        value = @chunks.shift
        raise EOFError unless value
        raise value if value.is_a?(Exception)

        value
      end
    end

    class CloseableIO < ReadOnlyIO
      attr_reader :read_calls

      def initialize(chunks)
        super
        @closed = false
        @read_calls = 0
      end

      def readpartial(chunk_size)
        @read_calls += 1
        value = super
        return value.byteslice(0, chunk_size) if value.is_a?(String)

        value
      end

      def close
        @closed = true
      end

      def closed?
        @closed
      end
    end

    class CloseRaisingIO < CloseableIO
      def close
        super
        raise "close boom"
      end
    end
  end
end
