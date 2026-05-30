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
