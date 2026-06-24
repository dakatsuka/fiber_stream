# frozen_string_literal: true

module FiberStream
  class Source
    RactorMergePortPair = Data.define(:port, :ack_port)
    private_constant :RactorMergePortPair

    # Creates a source definition from an Enumerable.
    #
    # The enumerable is not consumed until values are pulled by `run_with`. Each
    # materialization creates an Enumerator with `enumerable.to_enum(:each)`;
    # FiberStream does not snapshot values or guarantee replayability for
    # one-shot enumerables.
    def self.each(enumerable)
      new(-> { Pull.each(enumerable) })
    end

    # Creates a source definition from an IO-like object.
    #
    # The IO object is not read until values are pulled by `run_with`. Each
    # materialization reads from the same IO object's current position; this
    # source does not snapshot, reopen, or guarantee replayability. The IO is
    # closed only when `close: true` is passed. `chunk_size` is the maximum byte
    # count passed to `readpartial` for one downstream pull; very large values
    # may cause the IO implementation to attempt large allocations.
    def self.io(io, chunk_size: 16 * 1024, close: false)
      raise TypeError, "io must respond to readpartial" unless io.respond_to?(:readpartial)
      raise TypeError, "chunk_size must be an Integer" unless chunk_size.is_a?(Integer)
      raise ArgumentError, "chunk_size must be positive" unless chunk_size.positive?
      raise TypeError, "close must be true or false" unless [true, false].include?(close)
      raise TypeError, "io must respond to close" if close && !io.respond_to?(:close)

      new(-> { Pull.io(io, chunk_size, close) })
    end

    # Creates a backpressure-aware source definition from Ractor ports.
    #
    # `port` is the data/control port received by FiberStream. `ack_port` is a
    # producer-owned port that receives `RactorPort::Ack` and
    # `RactorPort::Cancel` control messages. The producer must wait for an ack
    # before sending each `RactorPort::Element`, `RactorPort::Complete`, or
    # `RactorPort::Failure` message. Failure metadata is producer-provided and
    # should be sanitized before crossing trust boundaries.
    def self.ractor_port(port, ack_port:, ack_transfer: :copy, cancel: true)
      raise TypeError, "port must respond to receive" unless port.respond_to?(:receive)
      unless ack_port.respond_to?(:send) && ack_port.method(:send).owner != Kernel
        raise TypeError, "ack_port must provide Ractor-style send"
      end

      Internal::RactorTransferPolicy.validate!(:ack_transfer, ack_transfer)
      raise TypeError, "cancel must be true or false" unless [true, false].include?(cancel)

      new(-> { Pull.ractor_port(port, ack_port, ack_transfer, cancel) })
    end

    # Creates a backpressure-aware source definition from multiple Ractor port
    # pairs.
    #
    # Each pair must be a Hash with `:port` and `:ack_port`. The source sends
    # at most one outstanding `RactorPort::Ack` to each active producer and
    # emits producer values in coordinator-observed ready order. Producer work
    # is isolated in Ractors, so demanding this source does not require a
    # `Fiber.scheduler`. Failure metadata is producer-provided and should be
    # sanitized before crossing trust boundaries.
    def self.ractor_merge_ports(ports, ack_transfer: :copy, cancel: true)
      pairs = normalize_ractor_merge_port_pairs(ports)

      Internal::RactorTransferPolicy.validate!(:ack_transfer, ack_transfer)
      raise TypeError, "cancel must be true or false" unless [true, false].include?(cancel)

      new(-> { Pull.ractor_merge_ports(pairs, ack_transfer, cancel) })
    end

    # Creates a source backed by one FiberStream-owned producer ractor.
    #
    # The producer ractor is started lazily on first downstream demand. The
    # shareable block receives a `RactorProducer` context and the provided
    # arguments. Calls to the context preserve one-outstanding-ack
    # backpressure, and cleanup always requests cooperative cancellation.
    def self.ractor_producer(*args, transfer: :copy, ack_transfer: :copy, &block)
      raise ArgumentError, "missing block" unless block

      Internal::RactorTransferPolicy.validate!(:transfer, transfer)
      Internal::RactorTransferPolicy.validate!(:ack_transfer, ack_transfer)
      raise TypeError, "block must be shareable" unless Ractor.shareable?(block)

      group = RactorProducerGroup.new(transfer)
      group.producer(*args, &block)
      definitions = group.definitions

      new(-> { Pull.ractor_producer(definitions, ack_transfer) })
    end

    # Creates a source backed by multiple FiberStream-owned producer ractors.
    #
    # The registration block runs at construction to collect producer
    # definitions, but producer ractors and ports are started lazily on first
    # downstream demand. Outputs are merged with the same ready-order semantics
    # as `Source.ractor_merge_ports`.
    def self.ractor_merge_producers(transfer: :copy, ack_transfer: :copy, &block)
      raise ArgumentError, "missing block" unless block

      Internal::RactorTransferPolicy.validate!(:transfer, transfer)
      Internal::RactorTransferPolicy.validate!(:ack_transfer, ack_transfer)

      group = RactorProducerGroup.new(transfer)
      block.call(group)
      definitions = group.definitions
      raise ArgumentError, "ractor_merge_producers requires at least two producers" if definitions.size < 2

      new(-> { Pull.ractor_merge_producers(definitions, ack_transfer) })
    end

    def self.build(source_factory, flows = []) # :nodoc:
      new(source_factory, flows)
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

      self.class.build(@source_factory, @flows + [flow])
    end

    # Returns a new source definition that emits this source, then `source`.
    #
    # Construction is lazy. The appended source is not materialized or pulled
    # until downstream demand observes completion from this source. Flows
    # attached before concat stay scoped to their source; flows attached after
    # concat apply to the combined output.
    def concat(source)
      raise TypeError, "expected FiberStream::Source" unless source.is_a?(Source)

      self.class.build(-> { Pull.concat(to_pull_materializer, source.to_pull_materializer) })
    end

    # Returns a new source definition that emits pairs from this source and
    # `source`.
    #
    # Construction is lazy. The receiver side is materialized only when
    # downstream demand reaches the zip stage; the other side is materialized
    # only after the receiver produces an element for a pair. The zipped source
    # completes when either input completes.
    def zip(source)
      raise TypeError, "expected FiberStream::Source" unless source.is_a?(Source)

      self.class.build(-> { Pull.zip(to_pull_materializer, source.to_pull_materializer) })
    end

    # Returns a new source definition that emits values from this source and
    # `source` in scheduler-observed ready order.
    #
    # Construction is lazy. The merged source starts one scheduled producer
    # fiber per input source only when downstream demand reaches the merge. Each
    # input's own element order is preserved, but cross-input ordering is not
    # deterministic and requires an installed `Fiber.scheduler` from a
    # non-blocking fiber when demanded.
    def merge(source)
      raise TypeError, "expected FiberStream::Source" unless source.is_a?(Source)

      self.class.build(-> { Pull.merge(to_pull_materializer, source.to_pull_materializer) })
    end

    # Returns a new source definition that maps each element with `block`.
    #
    # This is a convenience wrapper around `via(FiberStream::Flow.map { ... })`
    # and has the same lazy construction, error, and backpressure behavior as
    # the underlying flow.
    def map(&block)
      via(Flow.map(&block))
    end

    # Returns a new source definition that emits truthy transformed values.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.filter_map { ... })` and has the same falsey-drop,
    # lazy construction, error, and backpressure behavior as the underlying
    # flow.
    def filter_map(&block)
      via(Flow.filter_map(&block))
    end

    # Returns a new source definition that drops nil elements.
    #
    # This is a convenience wrapper around `via(FiberStream::Flow.compact)` and
    # preserves the same nil-only filtering, lazy construction, and
    # backpressure behavior as the underlying flow.
    def compact
      via(Flow.compact)
    end

    # Returns a new source definition that emits each mapped expansion.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.map_concat { ... })` and has the same one-level
    # flattening, lazy construction, error, and backpressure behavior as the
    # underlying flow.
    def map_concat(&block)
      via(Flow.map_concat(&block))
    end

    # Returns a new source definition that maps elements concurrently.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.parallel_map(concurrency:) { ... })` and preserves
    # the same ordered delivery, scheduler requirement, validation, bounded
    # upstream run-ahead, and cancellation behavior.
    def parallel_map(concurrency:, &block)
      via(Flow.parallel_map(concurrency: concurrency, &block))
    end

    # Returns a new source definition that maps elements concurrently and emits
    # mapped values in completion order.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.parallel_unordered_map(concurrency:) { ... })`.
    # The operation preserves the same scheduler requirement, validation,
    # bounded upstream run-ahead, and cancellation behavior while making no
    # input-order guarantee.
    def parallel_unordered_map(concurrency:, &block)
      via(Flow.parallel_unordered_map(concurrency: concurrency, &block))
    end

    # Returns a new source definition that maps elements in Ractor workers.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.ractor_map(workers:) { ... })` and preserves the
    # same shareable mapper requirement, ordered delivery, transfer policy,
    # bounded upstream run-ahead, and cooperative worker shutdown behavior.
    def ractor_map(workers:, input_transfer: :copy, output_transfer: :copy, &block)
      via(
        Flow.ractor_map(
          workers: workers,
          input_transfer: input_transfer,
          output_transfer: output_transfer,
          &block
        )
      )
    end

    # Returns a new source definition that maps elements in Ractor workers and
    # emits mapped values as workers complete.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.ractor_unordered_map(workers:) { ... })` and
    # preserves the same shareable mapper requirement, unordered delivery,
    # transfer policy, bounded upstream run-ahead, and cooperative worker
    # shutdown behavior.
    def ractor_unordered_map(workers:, input_transfer: :copy, output_transfer: :copy, &block)
      via(
        Flow.ractor_unordered_map(
          workers: workers,
          input_transfer: input_transfer,
          output_transfer: output_transfer,
          &block
        )
      )
    end

    # Returns a new source definition that keeps elements matching `block`.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.select { ... })` and has the same truthiness and
    # lazy construction behavior as the underlying flow.
    def select(&block)
      via(Flow.select(&block))
    end

    # Returns a new source definition that drops elements matching `block`.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.reject { ... })` and has the same truthiness and
    # lazy construction behavior as the underlying flow.
    def reject(&block)
      via(Flow.reject(&block))
    end

    # Returns a new source definition that emits at most `count` elements.
    #
    # This is a convenience wrapper around `via(FiberStream::Flow.take(count))`
    # and preserves the same validation and upstream close behavior.
    def take(count)
      via(Flow.take(count))
    end

    # Returns a new source definition that drops the first `count` elements.
    #
    # This is a convenience wrapper around `via(FiberStream::Flow.drop(count))`
    # and preserves the same validation and pull-driven backpressure behavior.
    def drop(count)
      via(Flow.drop(count))
    end

    # Returns a new source definition that groups adjacent elements into arrays.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.grouped(count))` and preserves the same validation,
    # ordering, final partial group, and pull-driven backpressure behavior.
    def grouped(count)
      via(Flow.grouped(count))
    end

    # Returns a new source definition that emits running accumulators.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.scan(initial) { ... })` and preserves the same
    # reducer order, lazy construction, and pull-driven backpressure behavior.
    def scan(initial, &block)
      via(Flow.scan(initial, &block))
    end

    # Returns a new source definition that emits leading elements while `block`
    # is truthy.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.take_while { ... })` and preserves the same
    # predicate truthiness, early completion, and upstream close behavior.
    def take_while(&block)
      via(Flow.take_while(&block))
    end

    # Returns a new source definition that drops leading elements while `block`
    # is truthy.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.drop_while { ... })` and preserves the same
    # predicate truthiness, prefix-dropping, and pass-through behavior.
    def drop_while(&block)
      via(Flow.drop_while(&block))
    end

    # Returns a new source definition with an asynchronous boundary.
    #
    # This is a convenience wrapper around `via(FiberStream::Flow.async)` and
    # preserves the same scheduler requirement and cancellation behavior.
    def async
      via(Flow.async)
    end

    # Returns a new source definition with a bounded asynchronous buffer.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.buffer(count))` and preserves the same validation,
    # scheduler requirement, and cancellation behavior.
    def buffer(count)
      via(Flow.buffer(count))
    end

    # Returns a new source definition that rate-limits emitted elements.
    #
    # This is a convenience wrapper around `via(FiberStream::Flow.throttle(...))`.
    # The `rate:` form creates a fresh default limiter for each materialization;
    # pass `limiter:` to share quota state across sources or runs.
    def throttle(**options)
      via(Flow.throttle(**options))
    end

    # Returns a new source definition that splits String chunks into lines.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.lines(chomp:, max_length:))`. With
    # `max_length: nil`, one unterminated line can buffer without bound. Set a
    # positive `max_length` for untrusted, network-facing, or otherwise
    # unbounded streams.
    def lines(chomp: true, max_length: nil)
      via(Flow.lines(chomp: chomp, max_length: max_length))
    end

    # Returns a new source definition that splits String chunks into frames.
    #
    # This is a convenience wrapper around
    # `via(FiberStream::Flow.split(separator, keep_separator:, max_length:))`.
    # With `max_length: nil`, one unterminated frame can buffer without bound.
    # Set a positive `max_length` for untrusted, network-facing, or otherwise
    # unbounded streams.
    def split(separator, keep_separator: false, max_length: nil)
      via(Flow.split(separator, keep_separator: keep_separator, max_length: max_length))
    end

    # Returns a runnable pipeline from this source to `sink`.
    #
    # Construction is lazy. The source and sink are not materialized until
    # `Pipeline#run` is called.
    def to(sink)
      raise TypeError, "expected FiberStream::Sink" unless sink.is_a?(Sink)

      Pipeline.build(self, sink)
    end

    # Materializes and runs this source with `sink`.
    #
    # The stream runs in the current fiber until completion or failure. The
    # method returns the sink's materialized value and closes the materialized
    # pull chain on success, failure, or early sink completion.
    def run_with(sink)
      raise TypeError, "expected FiberStream::Sink" unless sink.is_a?(Sink)

      primary_error = nil

      begin
        stream = materialize

        sink.run_stream(stream)
      rescue StandardError => error
        primary_error = error
        raise
      ensure
        begin
          stream&.close
        rescue StandardError => close_error
          raise close_error unless primary_error
        end
      end
    end

    private_class_method :new

    def to_pull_materializer # :nodoc:
      method(:materialize)
    end

    def self.normalize_ractor_merge_port_pairs(ports)
      raise TypeError, "ports must respond to each" unless ports.respond_to?(:each)

      data_port_ids = {}
      ack_port_ids = {}
      pairs =
        ports.each.map do |pair|
          normalize_ractor_merge_port_pair(pair, data_port_ids, ack_port_ids)
        end

      raise ArgumentError, "ractor_merge_ports requires at least two port pairs" if pairs.size < 2

      pairs.freeze
    end

    def self.normalize_ractor_merge_port_pair(pair, data_port_ids, ack_port_ids)
      raise TypeError, "port pair must be a Hash" unless pair.is_a?(Hash)
      raise TypeError, "port pair must include :port and :ack_port" unless pair.key?(:port) && pair.key?(:ack_port)

      port = pair.fetch(:port)
      ack_port = pair.fetch(:ack_port)
      raise TypeError, "port must respond to receive" unless port.respond_to?(:receive)
      unless ack_port.respond_to?(:send) && ack_port.method(:send).owner != Kernel
        raise TypeError, "ack_port must provide Ractor-style send"
      end

      port_id = port.object_id
      ack_port_id = ack_port.object_id
      raise ArgumentError, "data ports must be distinct" if data_port_ids.key?(port_id)
      raise ArgumentError, "ack ports must be distinct" if ack_port_ids.key?(ack_port_id)

      data_port_ids[port_id] = true
      ack_port_ids[ack_port_id] = true
      RactorMergePortPair.new(port:, ack_port:)
    end

    private_class_method :normalize_ractor_merge_port_pairs, :normalize_ractor_merge_port_pair

    private

    def materialize
      stream = nil

      begin
        stream = @source_factory.call
        @flows.each do |flow|
          stream = flow.attach_to(stream)
        end
        stream
      rescue StandardError
        begin
          stream&.close
        rescue StandardError
          nil
        end
        raise
      end
    end
  end
end
