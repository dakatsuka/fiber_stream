# frozen_string_literal: true

require_relative "../test_helper"

module FiberStream
  class FlowLinesTest < Minitest::Test
    def test_lines_splits_across_chunk_boundaries
      result =
        Source.each(["hel", "lo\nwor", "ld\n"])
          .via(Flow.lines)
          .run_with(Sink.to_a)

      assert_equal ["hello", "world"], result
    end

    def test_source_lines_convenience_delegates_to_flow_lines
      result =
        Source.each(["a\nb\n"])
          .lines
          .run_with(Sink.to_a)

      assert_equal ["a", "b"], result
    end

    def test_lines_emits_multiple_lines_from_one_chunk_over_multiple_pulls
      sink =
        Sink.__send__(:new) do |stream|
          [stream.next, stream.next, stream.next]
        end

      result =
        Source.each(["a\nb\nc\n"])
          .via(Flow.lines)
          .run_with(sink)

      assert_equal ["a", "b", "c"], result
    end

    def test_lines_emits_empty_lines
      result =
        Source.each(["\n\n"])
          .via(Flow.lines)
          .run_with(Sink.to_a)

      assert_equal ["", ""], result
    end

    def test_lines_emits_final_unterminated_line
      result =
        Source.each(["abc"])
          .via(Flow.lines)
          .run_with(Sink.to_a)

      assert_equal ["abc"], result
    end

    def test_lines_does_not_emit_empty_final_buffer_after_delimiter
      result =
        Source.each(["abc\n"])
          .via(Flow.lines)
          .run_with(Sink.to_a)

      assert_equal ["abc"], result
    end

    def test_lines_chomps_crlf_by_default
      result =
        Source.each(["a\r\nb\n"])
          .via(Flow.lines)
          .run_with(Sink.to_a)

      assert_equal ["a", "b"], result
    end

    def test_lines_treats_delimiter_as_byte_for_ascii_incompatible_chunks
      result =
        Source.each(["a\n".encode("UTF-16LE")])
          .via(Flow.lines(chomp: false))
          .run_with(Sink.to_a)

      assert_equal ["a\x00\n".b, "\x00".b], result
    end

    def test_lines_preserves_delimiters_when_chomp_is_false
      result =
        Source.each(["a\r\nb"])
          .via(Flow.lines(chomp: false))
          .run_with(Sink.to_a)

      assert_equal ["a\r\n", "b"], result
    end

    def test_lines_rejects_non_boolean_chomp
      error = assert_raises(TypeError) do
        Flow.lines(chomp: nil)
      end

      assert_match(/chomp must be true or false/, error.message)
    end

    def test_lines_rejects_non_integer_max_length
      error = assert_raises(TypeError) do
        Flow.lines(max_length: 1.5)
      end

      assert_match(/max_length must be nil or an Integer/, error.message)
    end

    def test_lines_rejects_zero_max_length
      error = assert_raises(ArgumentError) do
        Flow.lines(max_length: 0)
      end

      assert_match(/max_length must be positive/, error.message)
    end

    def test_lines_is_lazy
      pulled = false

      Source.each(["a\n"])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.lines)

      refute pulled
    end

    def test_lines_rejects_non_string_input
      error = assert_raises(TypeError) do
        Source.each([:not_string])
          .via(Flow.lines)
          .run_with(Sink.to_a)
      end

      assert_match(/Flow.lines elements must be String/, error.message)
    end

    def test_lines_allows_max_length_equal_to_delimited_line_bytesize
      result =
        Source.each(["abc\n"])
          .via(Flow.lines(max_length: 4))
          .run_with(Sink.to_a)

      assert_equal ["abc"], result
    end

    def test_lines_raises_when_max_length_is_exceeded
      error = assert_raises(FrameTooLongError) do
        Source.each(["abcd"])
          .via(Flow.lines(max_length: 3))
          .run_with(Sink.to_a)
      end

      assert_match(/frame exceeded max_length 3/, error.message)
    end

    def test_lines_applies_max_length_per_line_not_aggregate_buffer
      sink =
        Sink.__send__(:new) do |stream|
          first = stream.next
          assert_equal "ok", first
          stream.next
        end

      error = assert_raises(FrameTooLongError) do
        Source.each(["ok\nxxxxxxxxxxxx"])
          .via(Flow.lines(max_length: 10))
          .run_with(sink)
      end

      assert_match(/frame exceeded max_length 10/, error.message)
    end

    def test_lines_closes_upstream_when_max_length_is_exceeded
      closed = false

      assert_raises(FrameTooLongError) do
        Source.each(["abcd"])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.lines(max_length: 3))
          .run_with(Sink.to_a)
      end

      assert closed
    end

    def test_lines_prefers_frame_too_long_error_over_close_failure
      error = assert_raises(FrameTooLongError) do
        Source.each(["abcd"])
          .via(build_close_raising_flow)
          .via(Flow.lines(max_length: 3))
          .run_with(Sink.to_a)
      end

      assert_match(/frame exceeded max_length 3/, error.message)
    end

    private

    def build_next_counting_flow(&on_next)
      Flow.__send__(:new) do |upstream|
        NextCountingStage.new(upstream, &on_next)
      end
    end

    def build_close_tracking_flow(&on_close)
      Flow.__send__(:new) do |upstream|
        CloseTrackingStage.new(upstream, &on_close)
      end
    end

    def build_close_raising_flow
      Flow.__send__(:new) do |upstream|
        CloseRaisingStage.new(upstream)
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
