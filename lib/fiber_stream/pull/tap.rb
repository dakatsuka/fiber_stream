# frozen_string_literal: true

module FiberStream
  module Pull
    # Stateless observing stage.
    #
    # It pulls one upstream element for each downstream demand, calls the
    # observer for real elements, and emits the original element unchanged.
    class Tap
      def initialize(upstream, observer)
        @upstream = upstream
        @observer = observer
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

        @observer.call(value)
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
