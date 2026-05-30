# frozen_string_literal: true

module FiberStream
  class Flow
    # Creates a mapping flow.
    #
    # The block is called once for each element pulled through this flow.
    # Exceptions raised by the block fail the stream and are re-raised from
    # `Source#run_with`.
    def self.map(&block)
      raise ArgumentError, "missing block" unless block

      new { |upstream| Pull.map(upstream, block) }
    end

    # Creates a filtering flow.
    #
    # The block is called for upstream elements until it returns a truthy value
    # or upstream completes. Matching elements pass through unchanged.
    # Exceptions raised by the block fail the stream and are re-raised from
    # `Source#run_with`.
    def self.select(&block)
      raise ArgumentError, "missing block" unless block

      new { |upstream| Pull.select(upstream, block) }
    end

    # Creates a limiting flow.
    #
    # The flow emits at most `count` elements. `take(0)` completes without
    # pulling upstream and closes upstream on the first downstream demand. After
    # the limit is reached, upstream is closed during the pull that forwards
    # the final element. Negative counts raise `ArgumentError`; non-Integer
    # counts raise `TypeError`.
    def self.take(count)
      raise TypeError, "count must be an Integer" unless count.is_a?(Integer)
      raise ArgumentError, "count must be non-negative" if count.negative?

      new { |upstream| Pull.take(upstream, count) }
    end

    def initialize(&attach)
      @attach = attach
    end

    private_class_method :new

    private

    def attach(upstream)
      @attach.call(upstream)
    end
  end
end
