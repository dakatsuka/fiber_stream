# frozen_string_literal: true

module FiberStream
  class Source
    # Creates a source definition from an Enumerable.
    #
    # The enumerable is not consumed until values are pulled by `run_with`. Each
    # materialization creates an Enumerator with `enumerable.to_enum(:each)`;
    # FiberStream does not snapshot values or guarantee replayability for
    # one-shot enumerables.
    def self.each(enumerable)
      new(-> { Pull.each(enumerable) })
    end

    def initialize(source_factory, flows = [])
      @source_factory = source_factory
      @flows = flows
    end

    # Returns a new source definition that passes this source through `flow`.
    #
    # This method is lazy. It does not run the source, enumerate values, or call
    # flow blocks.
    def via(flow)
      raise TypeError, "expected FiberStream::Flow" unless flow.is_a?(Flow)

      self.class.new(@source_factory, @flows + [flow])
    end

    # Materializes and runs this source with `sink`.
    #
    # The stream runs in the current fiber until completion or failure. The
    # method returns the sink's materialized value and closes the materialized
    # pull chain on success, failure, or early sink completion.
    def run_with(sink)
      raise TypeError, "expected FiberStream::Sink" unless sink.is_a?(Sink)

      stream = @source_factory.call
      @flows.each do |flow|
        stream = flow.__send__(:attach, stream)
      end

      sink.__send__(:run, stream)
    ensure
      stream&.close
    end
  end
end
