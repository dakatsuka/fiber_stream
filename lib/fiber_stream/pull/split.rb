# frozen_string_literal: true

module FiberStream
  module Pull
    # Delimiter-framing stage for `Flow.split`.
    #
    # The stage keeps an internal byte buffer because frames and separators can
    # cross chunk boundaries. Length checks are per frame body, not against the
    # aggregate buffer, so already complete valid frames can be emitted before a
    # later over-limit frame fails.
    class Split
      def initialize(upstream, separator, keep_separator, max_length)
        @upstream = upstream
        @separator = separator.b.freeze
        @keep_separator = keep_separator
        @max_length = max_length
        @buffer = +"".b
        @closed = false
        @upstream_done = false
      end

      def next
        return DONE if @closed

        loop do
          frame = next_buffered_frame
          return frame if frame

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

      def next_buffered_frame
        separator_index = @buffer.index(@separator)
        return nil unless separator_index

        frame = @buffer.slice!(0, separator_index)
        @buffer.slice!(0, @separator.bytesize)
        validate_frame_length!(frame)
        format_frame(frame)
      end

      def complete_from_buffer
        return DONE if @buffer.empty?

        frame = @buffer
        @buffer = +"".b
        validate_frame_length!(frame)
        frame
      end

      def append_next_chunk
        chunk = @upstream.next
        if Pull.done?(chunk)
          @upstream_done = true
          return
        end

        unless chunk.is_a?(String)
          raise TypeError, "Flow.split elements must be String"
        end

        @buffer << chunk.b
        validate_pending_frame_length!
      end

      def validate_pending_frame_length!
        return unless @max_length
        return if pending_frame_body_bytesize <= @max_length

        fail_frame_too_long
      end

      def validate_frame_length!(frame)
        return unless @max_length
        return if frame.bytesize <= @max_length

        fail_frame_too_long
      end

      def pending_frame_body_bytesize
        separator_index = @buffer.index(@separator)
        return separator_index if separator_index

        @buffer.bytesize - partial_separator_suffix_bytesize
      end

      def partial_separator_suffix_bytesize
        max_suffix_bytesize = [@separator.bytesize - 1, @buffer.bytesize].min
        return 0 if max_suffix_bytesize.zero?

        max_suffix_bytesize.downto(1) do |bytesize|
          suffix = @buffer.byteslice(@buffer.bytesize - bytesize, bytesize)
          return bytesize if @separator.start_with?(suffix)
        end

        0
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

      def format_frame(frame)
        return frame unless @keep_separator

        frame + @separator
      end
    end
  end
end
