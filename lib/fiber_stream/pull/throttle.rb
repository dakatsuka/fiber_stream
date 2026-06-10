# frozen_string_literal: true

module FiberStream
  module Pull
    # Pull-driven rate-limiting stage.
    #
    # The stage pulls at most one upstream value, acquires one permit, and then
    # emits that value unless the stage was closed while waiting.
    class Throttle
      def initialize(upstream, limiter)
        @upstream = upstream
        @limiter = limiter
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

        @limiter.acquire(permits: 1)
        if @closed
          @done = true
          return DONE
        end

        value
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end
  end
end
