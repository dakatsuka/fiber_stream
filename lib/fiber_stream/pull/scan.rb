# frozen_string_literal: true

module FiberStream
  module Pull
    # Running-accumulator stage.
    #
    # It pulls one immediate upstream value for each downstream demand, updates
    # the accumulator with the reducer, and emits the updated accumulator.
    class Scan
      def initialize(upstream, initial, reducer)
        @upstream = upstream
        @accumulator = initial
        @reducer = reducer
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        value = @upstream.next
        if Pull.done?(value)
          @done = true
          return DONE
        end

        @accumulator = @reducer.call(@accumulator, value)
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end
  end
end
