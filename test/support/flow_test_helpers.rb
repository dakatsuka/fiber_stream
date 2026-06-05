# frozen_string_literal: true

module FiberStream
  module FlowTestHelpers
    def build_close_tracking_flow(&on_close)
      Flow.__send__(:new) do |upstream|
        CloseTrackingStage.new(upstream, &on_close)
      end
    end

    def build_close_raising_flow(&on_close)
      Flow.__send__(:new) do |upstream|
        CloseRaisingStage.new(upstream, &on_close)
      end
    end

    def build_next_counting_flow(&on_next)
      Flow.__send__(:new) do |upstream|
        NextCountingStage.new(upstream, &on_next)
      end
    end

    def build_next_raising_flow(raise_on_call:)
      Flow.__send__(:new) do |upstream|
        NextRaisingStage.new(upstream, raise_on_call)
      end
    end

    def build_repeated_pull_sink(count)
      Sink.__send__(:new) do |stream|
        count.times.map { stream.next }
      end
    end

    class CloseTrackingStage
      def initialize(upstream, &on_close)
        @upstream = upstream
        @on_close = on_close
        @closed = false
      end

      def next
        @upstream.next
      end

      def close
        return if @closed

        @closed = true
        @on_close.call
        @upstream.close
      end
    end

    class CloseRaisingStage
      def initialize(upstream, &on_close)
        @upstream = upstream
        @on_close = on_close || -> {}
        @closed = false
      end

      def next
        @upstream.next
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
        @on_close.call
        raise "close boom"
      end
    end

    class NextCountingStage
      def initialize(upstream, &on_next)
        @upstream = upstream
        @on_next = on_next
      end

      def next
        @on_next.call
        @upstream.next
      end

      def close
        @upstream.close
      end
    end

    class NextRaisingStage
      def initialize(upstream, raise_on_call)
        @upstream = upstream
        @raise_on_call = raise_on_call
        @calls = 0
      end

      def next
        @calls += 1
        raise "next boom" if @calls == @raise_on_call

        @upstream.next
      end

      def close
        @upstream.close
      end
    end
  end
end
