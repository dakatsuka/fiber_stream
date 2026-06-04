# frozen_string_literal: true

module FiberStream
  module Pull
    # Fixed-prefix dropping stage.
    #
    # It discards the first `count` upstream elements on downstream demand, then
    # passes later elements through without buffering.
    class Drop
      def initialize(upstream, count)
        @upstream = upstream
        @remaining = count
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        drop_prefix
        return DONE if @done

        pull_retained_value
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end

      private

      def drop_prefix
        while @remaining.positive?
          value = @upstream.next
          if Pull.done?(value)
            @done = true
            return
          end

          @remaining -= 1
        end
      end

      def pull_retained_value
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
