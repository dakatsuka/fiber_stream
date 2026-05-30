# frozen_string_literal: true

module FiberStream
  module Pull
    DONE = Object.new.freeze
    private_constant :DONE

    def self.done?(value)
      value.equal?(DONE)
    end

    def self.each(enumerable)
      Each.new(enumerable)
    end

    def self.map(upstream, transform)
      Map.new(upstream, transform)
    end

    def self.select(upstream, predicate)
      Select.new(upstream, predicate)
    end

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

    class Map
      def initialize(upstream, transform)
        @upstream = upstream
        @transform = transform
        @closed = false
      end

      def next
        return DONE if @closed

        value = @upstream.next
        return DONE if Pull.done?(value)

        @transform.call(value)
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end

    class Select
      def initialize(upstream, predicate)
        @upstream = upstream
        @predicate = predicate
        @closed = false
      end

      def next
        return DONE if @closed

        loop do
          value = @upstream.next
          return DONE if Pull.done?(value)
          return value if @predicate.call(value)
        end
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end

    private_constant :DONE, :Each, :Map, :Select
  end
end
