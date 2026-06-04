# frozen_string_literal: true

module FiberStream
  module Pull
    # Predicate-based limiting stage.
    #
    # It forwards the leading prefix whose predicate results are truthy. The
    # first falsey predicate result is consumed, not emitted, and closes
    # upstream immediately.
    class TakeWhile
      def initialize(upstream, predicate)
        @upstream = upstream
        @predicate = predicate
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

        return value if @predicate.call(value)

        @done = true
        close
        DONE
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end
  end
end
