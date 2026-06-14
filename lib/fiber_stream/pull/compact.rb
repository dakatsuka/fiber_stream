# frozen_string_literal: true

module FiberStream
  module Pull
    # Nil-dropping stage.
    #
    # A single downstream demand may pull multiple upstream elements until a
    # non-nil value is observed or upstream completes. Dropped nil values are
    # discarded immediately and are not buffered.
    class Compact
      def initialize(upstream)
        @upstream = upstream
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

          return value unless value.nil?
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
