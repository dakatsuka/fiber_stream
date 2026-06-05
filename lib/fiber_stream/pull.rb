# frozen_string_literal: true

module FiberStream
  # Internal pull-stream runtime.
  #
  # Public `Source`, `Flow`, and `Sink` objects are lazy definitions. When a
  # source is materialized, those definitions attach together into private pull
  # stream objects from this module. Every pull stream responds to `#next` and
  # `#close`: `#next` returns either an element or the private `DONE` sentinel,
  # while `#close` releases upstream resources and may raise cleanup failures.
  # The sentinel must never escape through public APIs.
  module Pull
    DONE = Object.new.freeze

    def self.done?(value)
      value.equal?(DONE)
    end

    def self.each(enumerable)
      Each.new(enumerable)
    end

    def self.io(io, chunk_size, close_io)
      IOSource.new(io, chunk_size, close_io)
    end

    def self.ractor_port(port, ack_port, ack_transfer, cancel)
      RactorPortSource.new(port, ack_port, ack_transfer, cancel)
    end

    def self.ractor_merge_ports(port_pairs, ack_transfer, cancel)
      RactorMergePortsSource.new(port_pairs, ack_transfer, cancel)
    end

    def self.concat(left_materializer, right_materializer)
      Concat.new(left_materializer, right_materializer)
    end

    def self.zip(left_materializer, right_materializer)
      Zip.new(left_materializer, right_materializer)
    end

    def self.merge(left_materializer, right_materializer)
      Merge.new(left_materializer, right_materializer)
    end

    def self.map(upstream, transform)
      Map.new(upstream, transform)
    end

    def self.parallel_map(upstream, concurrency, transform)
      ParallelMapBoundary.new(upstream, concurrency, transform)
    end

    def self.ractor_map(upstream, workers, input_transfer, output_transfer, transform)
      RactorMapBoundary.new(upstream, workers, input_transfer, output_transfer, transform)
    end

    def self.select(upstream, predicate)
      Select.new(upstream, predicate)
    end

    def self.take(upstream, count)
      Take.new(upstream, count)
    end

    def self.drop(upstream, count)
      Drop.new(upstream, count)
    end

    def self.grouped(upstream, count)
      Grouped.new(upstream, count)
    end

    def self.take_while(upstream, predicate)
      TakeWhile.new(upstream, predicate)
    end

    def self.drop_while(upstream, predicate)
      DropWhile.new(upstream, predicate)
    end

    def self.async(upstream)
      AsyncBoundary.new(upstream)
    end

    def self.buffer(upstream, count)
      BufferBoundary.new(upstream, count)
    end

    def self.lines(upstream, chomp, max_length)
      Lines.new(upstream, chomp, max_length)
    end

    private_constant :DONE
  end
end

require_relative "pull/each"
require_relative "pull/io_source"
require_relative "pull/ractor_port_source"
require_relative "pull/ractor_merge_ports_source"
require_relative "pull/concat"
require_relative "pull/zip"
require_relative "pull/merge"
require_relative "pull/map"
require_relative "pull/select"
require_relative "pull/take"
require_relative "pull/drop"
require_relative "pull/grouped"
require_relative "pull/take_while"
require_relative "pull/drop_while"
require_relative "pull/lines"
require_relative "pull/async_boundary"
require_relative "pull/buffer_boundary"
require_relative "pull/parallel_map_boundary"
require_relative "pull/ractor_map_boundary"

module FiberStream
  module Pull
    private_constant :Each, :IOSource, :RactorPortSource, :RactorMergePortsSource, :Concat, :Zip, :Merge, :Map,
                     :Select, :Take, :Drop, :Grouped, :TakeWhile, :DropWhile, :Lines, :AsyncBoundary,
                     :BufferBoundary, :ParallelMapBoundary, :RactorMapBoundary
  end
end
