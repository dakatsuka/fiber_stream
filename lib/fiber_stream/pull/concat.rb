# frozen_string_literal: true

module FiberStream
  module Pull
    # Pull stream that emits all values from one materialized source, then all
    # values from a second source materialized only after the first completes.
    class Concat
      def initialize(left_materializer, right_materializer)
        @left_materializer = left_materializer
        @right_materializer = right_materializer
        @left = @left_materializer.call
        @right = nil
        @phase = :left
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        case @phase
        when :left
          next_left
        when :right
          next_right
        else
          DONE
        end
      end

      def close
        return if @closed

        @closed = true
        close_materialized_streams
      end

      private

      def next_left
        value = @left.next
        return value unless Pull.done?(value)

        close_left
        @phase = :right
        @right = @right_materializer.call
        next_right
      end

      def next_right
        value = @right.next
        return value unless Pull.done?(value)

        close_right
        @done = true
        DONE
      end

      def close_left
        stream = @left
        return unless stream

        stream.close
        @left = nil
      end

      def close_right
        stream = @right
        return unless stream

        stream.close
        @right = nil
      end

      def close_materialized_streams
        first_error = nil

        [@right, @left].each do |stream|
          next unless stream

          begin
            stream.close
          rescue StandardError => error
            first_error ||= error
          end
        end

        @right = nil
        @left = nil

        raise first_error if first_error
      end
    end
  end
end
