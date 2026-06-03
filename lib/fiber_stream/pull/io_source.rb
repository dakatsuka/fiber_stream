# frozen_string_literal: true

module FiberStream
  module Pull
    # Pull stream for `Source.io`.
    #
    # Reads are demand-driven: every downstream `#next` performs at most one
    # `readpartial` call. The stream owns IO close only when `close_io` is true,
    # and preserves primary stream failures over cleanup close failures.
    class IOSource
      def initialize(io, chunk_size, close_io)
        @io = io
        @chunk_size = chunk_size
        @close_io = close_io
        @closed = false
        @done = false
        @io_closed = false
      end

      def next
        return DONE if @closed || @done

        validate_scheduler!

        chunk = read_chunk
        return DONE if Pull.done?(chunk)
        return chunk if chunk.is_a?(String)

        fail_with_primary(TypeError.new("readpartial must return a String"))
      end

      def close
        return if @closed

        @closed = true
        @done = true
        close_error = close_io
        raise close_error if close_error
      end

      private

      def validate_scheduler!
        return if Fiber.scheduler && !Fiber.current.blocking?

        message =
          if Fiber.scheduler
            "Source.io requires a non-blocking fiber"
          else
            "Source.io requires Fiber.scheduler"
          end
        fail_with_primary(SchedulerRequiredError.new(message))
      end

      def read_chunk
        @io.readpartial(@chunk_size)
      rescue EOFError
        complete
      rescue StandardError => error
        fail_with_primary(error)
      end

      def complete
        @done = true
        close_error = close_io
        raise close_error if close_error

        DONE
      end

      def fail_with_primary(error)
        @done = true
        close_io
        raise error
      end

      def close_io
        return nil unless @close_io
        return nil if @io_closed

        @io_closed = true
        @io.close
        nil
      rescue StandardError => error
        error
      end
    end
  end
end
