# frozen_string_literal: true

module FiberStream
  module Pull
    # Transform-and-filter stage.
    #
    # A single downstream demand may pull multiple upstream elements until the
    # transform returns a truthy value or upstream completes. Falsey transform
    # results are discarded immediately and are not buffered.
    class FilterMap
      def initialize(upstream, transform)
        @upstream = upstream
        @transform = transform
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        loop do
          value = @upstream.next
          if Pull.done?(value)
            @done = true
            return DONE
          end

          result = @transform.call(value)
          return result if result
        end
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end
  end
end
