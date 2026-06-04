# frozen_string_literal: true

module FiberStream
  module Pull
    # Predicate-based prefix dropping stage.
    #
    # It drops leading elements while the predicate is truthy. The first falsey
    # element and all later elements pass through unchanged.
    class DropWhile
      def initialize(upstream, predicate)
        @upstream = upstream
        @predicate = predicate
        @dropping = true
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        return pull_pass_through unless @dropping

        pull_until_retained
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end

      private

      def pull_until_retained
        loop do
          value = @upstream.next
          if Pull.done?(value)
            @done = true
            return DONE
          end

          next if @predicate.call(value)

          @dropping = false
          return value
        end
      end

      def pull_pass_through
        value = @upstream.next
        if Pull.done?(value)
          @done = true
          return DONE
        end

        value
      end
    end
  end
end
