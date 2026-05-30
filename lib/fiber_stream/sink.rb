# frozen_string_literal: true

module FiberStream
  class Sink
    # Creates a sink that collects all stream elements into an Array.
    #
    # The sink consumes upstream until normal completion and returns the
    # collected array as the stream materialized value.
    def self.to_a
      new do |stream|
        values = []

        loop do
          value = stream.next
          break if Pull.done?(value)

          values << value
        end

        values
      end
    end

    def initialize(&run)
      @run = run
    end

    private_class_method :new

    private

    def run(stream)
      @run.call(stream)
    end
  end
end
