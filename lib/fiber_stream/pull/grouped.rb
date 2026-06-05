# frozen_string_literal: true

module FiberStream
  module Pull
    # Fixed-size grouping stage.
    #
    # It collects adjacent upstream elements into distinct arrays of up to
    # `count` elements. A final partial group is emitted when upstream completes
    # normally.
    class Grouped
      def initialize(upstream, count)
        @upstream = upstream
        @count = count
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        group = []

        while group.length < @count
          value = @upstream.next
          if Pull.done?(value)
            @done = true
            return DONE if group.empty?

            return group
          end

          group << value
        end

        group
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end
  end
end
