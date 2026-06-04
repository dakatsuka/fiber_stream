# frozen_string_literal: true

module FiberStream
  module Pull
    # Pull stream that emits pairs from two source definitions.
    #
    # The receiver side is materialized on first downstream demand. The other
    # side is materialized only after the receiver produces a value for a pair.
    class Zip
      def initialize(left_materializer, right_materializer)
        @left_materializer = left_materializer
        @right_materializer = right_materializer
        @left = nil
        @right = nil
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        left = materialize_left
        left_value = left.next
        if Pull.done?(left_value)
          @done = true
          close_materialized_streams
          return DONE
        end

        right = materialize_right
        right_value = right.next
        if Pull.done?(right_value)
          @done = true
          close_materialized_streams
          return DONE
        end

        [left_value, right_value]
      rescue StandardError
        @done = true
        close_materialized_streams(raise_error: false)
        raise
      end

      def close
        return if @closed

        @closed = true
        close_materialized_streams
      end

      private

      def materialize_left
        @left ||= @left_materializer.call
      end

      def materialize_right
        @right ||= @right_materializer.call
      end

      def close_materialized_streams(raise_error: true)
        streams = [@left, @right]
        @left = nil
        @right = nil

        first_error = nil

        streams.each do |stream|
          next unless stream

          begin
            stream.close
          rescue StandardError => error
            first_error ||= error
          end
        end

        raise first_error if raise_error && first_error
      end
    end
  end
end
