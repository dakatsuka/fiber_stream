# frozen_string_literal: true

require_relative "../test_helper"

module FiberStream
  class FlowSplitTest < Minitest::Test
    def test_split_splits_across_chunk_boundaries
      result =
        Source.each(["hel", "lo,wor", "ld,"])
          .via(Flow.split(","))
          .run_with(Sink.to_a)

      assert_equal ["hello", "world"], result
    end

    def test_source_split_convenience_delegates_to_flow_split
      result =
        Source.each(["a,b,"])
          .split(",")
          .run_with(Sink.to_a)

      assert_equal ["a", "b"], result
    end

    def test_split_emits_multiple_frames_from_one_chunk_over_multiple_pulls
      sink =
        Sink.__send__(:new) do |stream|
          [stream.next, stream.next, stream.next]
        end

      result =
        Source.each(["a,b,c,"])
          .via(Flow.split(","))
          .run_with(sink)

      assert_equal ["a", "b", "c"], result
    end

    def test_split_emits_empty_frames_for_consecutive_separators
      result =
        Source.each(["||"])
          .via(Flow.split("|"))
          .run_with(Sink.to_a)

      assert_equal ["", ""], result
    end

    def test_split_emits_interior_empty_frame_before_trailing_separator
      result =
        Source.each(["a||"])
          .via(Flow.split("|"))
          .run_with(Sink.to_a)

      assert_equal ["a", ""], result
    end

    def test_split_emits_single_empty_frame_for_single_separator
      result =
        Source.each(["|"])
          .via(Flow.split("|"))
          .run_with(Sink.to_a)

      assert_equal [""], result
    end

    def test_split_does_not_emit_frames_for_empty_input
      result =
        Source.each([])
          .via(Flow.split("|"))
          .run_with(Sink.to_a)

      assert_equal [], result
    end

    def test_split_emits_final_unterminated_frame
      result =
        Source.each(["abc"])
          .via(Flow.split(","))
          .run_with(Sink.to_a)

      assert_equal ["abc"], result
    end

    def test_split_does_not_emit_empty_final_buffer_after_separator
      result =
        Source.each(["abc,"])
          .via(Flow.split(","))
          .run_with(Sink.to_a)

      assert_equal ["abc"], result
    end

    def test_split_keeps_separators_when_requested
      result =
        Source.each(["a--b"])
          .via(Flow.split("--", keep_separator: true))
          .run_with(Sink.to_a)

      assert_equal ["a--", "b"], result
    end

    def test_split_keeps_separator_for_empty_terminated_frame
      result =
        Source.each(["|"])
          .via(Flow.split("|", keep_separator: true))
          .run_with(Sink.to_a)

      assert_equal ["|"], result
    end

    def test_split_handles_separator_across_chunk_boundaries
      result =
        Source.each(["a<", "<b<<"])
          .via(Flow.split("<<"))
          .run_with(Sink.to_a)

      assert_equal ["a", "b"], result
    end

    def test_split_uses_leftmost_non_overlapping_separator_matches
      result =
        Source.each(["ababa"])
          .via(Flow.split("aba"))
          .run_with(Sink.to_a)

      assert_equal ["", "ba"], result
    end

    def test_split_treats_separator_as_byte_for_ascii_incompatible_chunks
      result =
        Source.each(["a,".encode("UTF-16LE")])
          .via(Flow.split(","))
          .run_with(Sink.to_a)

      assert_equal ["a\x00".b, "\x00".b], result
    end

    def test_split_does_not_chomp_cr_before_newline
      result =
        Source.each(["a\r\n"])
          .via(Flow.split("\n"))
          .run_with(Sink.to_a)

      assert_equal ["a\r"], result
    end

    def test_split_rejects_non_string_separator
      error = assert_raises(TypeError) do
        Flow.split(:comma)
      end

      assert_match(/separator must be String/, error.message)
    end

    def test_split_rejects_empty_separator
      error = assert_raises(ArgumentError) do
        Flow.split("")
      end

      assert_match(/separator must not be empty/, error.message)
    end

    def test_split_rejects_non_boolean_keep_separator
      error = assert_raises(TypeError) do
        Flow.split(",", keep_separator: nil)
      end

      assert_match(/keep_separator must be true or false/, error.message)
    end

    def test_split_rejects_non_integer_max_length
      error = assert_raises(TypeError) do
        Flow.split(",", max_length: 1.5)
      end

      assert_match(/max_length must be nil or an Integer/, error.message)
    end

    def test_split_rejects_zero_max_length
      error = assert_raises(ArgumentError) do
        Flow.split(",", max_length: 0)
      end

      assert_match(/max_length must be positive/, error.message)
    end

    def test_split_rejects_negative_max_length
      error = assert_raises(ArgumentError) do
        Flow.split(",", max_length: -1)
      end

      assert_match(/max_length must be positive/, error.message)
    end

    def test_split_is_lazy
      pulled = false

      Source.each(["a,"])
        .via(build_next_counting_flow { pulled = true })
        .via(Flow.split(","))

      refute pulled
    end

    def test_split_rejects_non_string_input
      error = assert_raises(TypeError) do
        Source.each([:not_string])
          .via(Flow.split(","))
          .run_with(Sink.to_a)
      end

      assert_match(/Flow.split elements must be String/, error.message)
    end

    def test_split_allows_max_length_equal_to_frame_body_bytesize
      result =
        Source.each(["abc,"])
          .via(Flow.split(",", max_length: 3))
          .run_with(Sink.to_a)

      assert_equal ["abc"], result
    end

    def test_split_max_length_excludes_separator_when_kept
      result =
        Source.each(["abc,"])
          .via(Flow.split(",", keep_separator: true, max_length: 3))
          .run_with(Sink.to_a)

      assert_equal ["abc,"], result
    end

    def test_split_raises_when_max_length_is_exceeded
      error = assert_raises(FrameTooLongError) do
        Source.each(["abcd"])
          .via(Flow.split(",", max_length: 3))
          .run_with(Sink.to_a)
      end

      assert_match(/frame exceeded max_length 3/, error.message)
    end

    def test_split_applies_max_length_per_frame_not_aggregate_buffer
      sink =
        Sink.__send__(:new) do |stream|
          first = stream.next
          assert_equal "ok", first
          stream.next
        end

      error = assert_raises(FrameTooLongError) do
        Source.each(["ok,xxxx"])
          .via(Flow.split(",", max_length: 2))
          .run_with(sink)
      end

      assert_match(/frame exceeded max_length 2/, error.message)
    end

    def test_split_does_not_count_partial_separator_suffix_as_pending_frame_body
      result =
        Source.each(["a-", "-"])
          .via(Flow.split("--", max_length: 1))
          .run_with(Sink.to_a)

      assert_equal ["a"], result
    end

    def test_split_closes_upstream_when_max_length_is_exceeded
      closed = false

      assert_raises(FrameTooLongError) do
        Source.each(["abcd"])
          .via(build_close_tracking_flow { closed = true })
          .via(Flow.split(",", max_length: 3))
          .run_with(Sink.to_a)
      end

      assert closed
    end

    def test_split_prefers_frame_too_long_error_over_close_failure
      error = assert_raises(FrameTooLongError) do
        Source.each(["abcd"])
          .via(build_close_raising_flow)
          .via(Flow.split(",", max_length: 3))
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
        raise "close failed"
      end
    end
  end
end
