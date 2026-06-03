# frozen_string_literal: true

module FiberStream
  module Pull
    # Line-framing stage for `Flow.lines`.
    #
    # The stage keeps an internal byte buffer because line boundaries can cross
    # chunk boundaries. Length checks are per line/frame, not against the
    # aggregate buffer, so already complete valid lines can be emitted before a
    # later over-limit line fails.
    class Lines
      NEWLINE = "\n".b
      CARRIAGE_RETURN = "\r".b

      def initialize(upstream, chomp, max_length)
        @upstream = upstream
        @chomp = chomp
        @max_length = max_length
        @buffer = +"".b
        @closed = false
        @upstream_done = false
      end

      def next
        return DONE if @closed

        loop do
          line = next_buffered_line
          return line if line

          validate_pending_frame_length!
          return complete_from_buffer if @upstream_done

          append_next_chunk
        end
      end

      def close
        return if @closed

        @closed = true
        @buffer.clear
        @upstream.close
      end

      private

      def next_buffered_line
        newline_index = @buffer.index(NEWLINE)
        return nil unless newline_index

        frame = @buffer.slice!(0, newline_index + 1)
        validate_frame_length!(frame)
        format_frame(frame, terminated: true)
      end

      def complete_from_buffer
        return DONE if @buffer.empty?

        frame = @buffer
        @buffer = +"".b
        validate_frame_length!(frame)
        format_frame(frame, terminated: false)
      end

      def append_next_chunk
        chunk = @upstream.next
        if Pull.done?(chunk)
          @upstream_done = true
          return
        end

        unless chunk.is_a?(String)
          raise TypeError, "Flow.lines elements must be String"
        end

        @buffer << chunk.b
        validate_pending_frame_length!
      end

      def validate_pending_frame_length!
        return unless @max_length
        return if @buffer.include?(NEWLINE)
        return if @buffer.bytesize <= @max_length

        fail_frame_too_long
      end

      def validate_frame_length!(frame)
        return unless @max_length
        return if frame.bytesize <= @max_length

        fail_frame_too_long
      end

      def fail_frame_too_long
        @closed = true
        close_upstream
        error = FrameTooLongError.new("frame exceeded max_length #{@max_length}")
        raise error
      end

      def close_upstream
        @upstream.close
        nil
      rescue StandardError => error
        error
      end

      def format_frame(frame, terminated:)
        return frame unless @chomp && terminated

        frame = frame.byteslice(0, frame.bytesize - 1)
        if frame.end_with?(CARRIAGE_RETURN)
          frame = frame.byteslice(0, frame.bytesize - 1)
        end
        frame
      end
    end
  end
end
