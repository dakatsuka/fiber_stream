# frozen_string_literal: true

module FiberStream
  module Pull
    # One-to-many mapping stage.
    #
    # It expands one upstream element into the values yielded by one returned
    # `#each` object. Only one expansion is active at a time, and the stage
    # never pulls the next upstream element until the active expansion is
    # exhausted.
    class MapConcat
      def initialize(upstream, transform)
        @upstream = upstream
        @transform = transform
        @current_enumerator = nil
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        loop do
          if @current_enumerator
            begin
              return @current_enumerator.next
            rescue StopIteration
              @current_enumerator = nil
            end
          end

          value = @upstream.next
          if Pull.done?(value)
            @done = true
            return DONE
          end

          result = @transform.call(value)
          unless result.respond_to?(:each)
            raise TypeError, "map_concat block result must respond to each"
          end

          @current_enumerator = result.to_enum(:each)
        end
      end

      def close
        return if @closed

        @closed = true
        @current_enumerator = nil
        @upstream.close
      end
    end
  end
end
