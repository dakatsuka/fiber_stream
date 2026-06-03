# frozen_string_literal: true

module FiberStream
  module Pull
    # Stateless mapping stage.
    #
    # It pulls one upstream element for each downstream demand and applies the
    # transform only to real elements, never to the `DONE` sentinel.
    class Map
      def initialize(upstream, transform)
        @upstream = upstream
        @transform = transform
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

        @transform.call(value)
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end
  end
end
