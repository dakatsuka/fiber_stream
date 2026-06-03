# frozen_string_literal: true

module FiberStream
  module Pull
    # Pull stream for `Source.each`.
    #
    # It owns only the per-materialization Enumerator created from the supplied
    # enumerable. The original enumerable remains caller-owned and is never
    # closed by FiberStream.
    class Each
      def initialize(enumerable)
        @iterator = enumerable.to_enum(:each)
        @closed = false
      end

      def next
        return DONE if @closed

        @iterator.next
      rescue StopIteration
        DONE
      end

      def close
        return if @closed

        @closed = true
      end
    end
  end
end
