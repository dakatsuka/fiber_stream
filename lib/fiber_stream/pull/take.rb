# frozen_string_literal: true

module FiberStream
  module Pull
    # Limiting stage.
    #
    # The stage closes upstream as soon as the limit is reached, including
    # `take(0)` on first demand. This makes early completion visible to
    # resource-owning sources and asynchronous boundaries.
    class Take
      def initialize(upstream, count)
        @upstream = upstream
        @remaining = count
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        if @remaining.zero?
          @done = true
          close
          return DONE
        end

        value = @upstream.next
        if Pull.done?(value)
          @done = true
          return DONE
        end

        @remaining -= 1
        close if @remaining.zero?

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
