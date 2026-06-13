# frozen_string_literal: true

module FiberStream
  module Pull
    # Complement filtering stage.
    #
    # A single downstream demand may pull multiple upstream elements until the
    # predicate retains a value or upstream completes. Rejected elements are
    # discarded immediately and are not buffered.
    class Reject
      def initialize(upstream, predicate)
        @upstream = upstream
        @predicate = predicate
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

          return value unless @predicate.call(value)
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
