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

    # Creates a transform-and-filter flow.
    #
    # The block is called once for each upstream element observed by this
    # stage. Truthy block results are emitted downstream as transformed values;
    # false and nil results are dropped. Exceptions raised by the block fail the
    # stream and are re-raised from `Source#run_with`.
    def self.filter_map(&block)
      raise ArgumentError, "missing block" unless block

      new { |upstream| Pull.filter_map(upstream, block) }
    end

    # Creates a pass-through observing flow.
    #
    # The block is called once for each element before that element is emitted
    # downstream. The block return value is ignored and the original element is
    # passed through unchanged. Exceptions raised by the block fail the stream
    # and are re-raised from `Source#run_with`.
    def self.tap(&block)
      raise ArgumentError, "missing block" unless block

      new { |upstream| Pull.tap(upstream, block) }
    end

    # Creates an ordered scheduler-backed parallel mapping flow.
    #
    # The stage starts internal scheduled fibers on first downstream demand and
    # requires an installed `Fiber.scheduler` in a non-blocking fiber at that
    # point. At most `concurrency` mapping blocks run at the same time, and at
    # most `concurrency` upstream elements are pulled but not yet emitted downstream.
    # Results are emitted in input order. Closing the boundary closes upstream
    # and requests internal worker cancellation. FiberStream does not depend on
    # Async at runtime.
    def self.parallel_map(concurrency:, &block)
      raise ArgumentError, "missing block" unless block
      raise TypeError, "concurrency must be an Integer" unless concurrency.is_a?(Integer)
      raise ArgumentError, "concurrency must be positive" unless concurrency.positive?

      new { |upstream| Pull.parallel_map(upstream, concurrency, block) }
    end

    # Creates an unordered scheduler-backed parallel mapping flow.
    #
    # The stage starts internal scheduled fibers on first downstream demand and
    # requires an installed `Fiber.scheduler` in a non-blocking fiber at that
    # point. At most `concurrency` mapping blocks run at the same time, and at
    # most `concurrency` upstream elements are pulled but not yet emitted downstream.
    # Results are emitted in completion order and input order is not preserved.
    # Closing the boundary closes upstream and requests internal worker
    # cancellation. FiberStream does not depend on Async at runtime.
    def self.parallel_unordered_map(concurrency:, &block)
      raise ArgumentError, "missing block" unless block
      raise TypeError, "concurrency must be an Integer" unless concurrency.is_a?(Integer)
      raise ArgumentError, "concurrency must be positive" unless concurrency.positive?

      new { |upstream| Pull.parallel_unordered_map(upstream, concurrency, block) }
    end

    # Creates an ordered Ractor-backed mapping flow.
    #
    # The mapper runs inside worker ractors and must be shareable, typically
    # created with `Ractor.shareable_proc`. Results are emitted in input order,
    # and at most `workers` upstream elements are pulled but not yet emitted.
    # `input_transfer` and `output_transfer` must be `:copy` or `:move` and are
    # passed to Ractor message sends for element and result transfer.
    def self.ractor_map(workers:, input_transfer: :copy, output_transfer: :copy, &block)
      raise ArgumentError, "missing block" unless block
      raise TypeError, "workers must be an Integer" unless workers.is_a?(Integer)
      raise ArgumentError, "workers must be positive" unless workers.positive?

      Internal::RactorTransferPolicy.validate!(:input_transfer, input_transfer)
      Internal::RactorTransferPolicy.validate!(:output_transfer, output_transfer)
      raise TypeError, "block must be shareable" unless Ractor.shareable?(block)

      new { |upstream| Pull.ractor_map(upstream, workers, input_transfer, output_transfer, block) }
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

    # Creates a fixed-prefix dropping flow.
    #
    # The flow discards the first `count` upstream elements, then passes later
    # elements through unchanged. `drop(0)` behaves as pass-through. Negative
    # counts raise `ArgumentError`; non-Integer counts raise `TypeError`.
    def self.drop(count)
      raise TypeError, "count must be an Integer" unless count.is_a?(Integer)
      raise ArgumentError, "count must be non-negative" if count.negative?

      new { |upstream| Pull.drop(upstream, count) }
    end

    # Creates a fixed-size grouping flow.
    #
    # The flow emits arrays containing up to `count` adjacent upstream elements.
    # Full groups contain exactly `count` elements; normal upstream completion
    # emits one final partial group when one exists. `count` must be a positive
    # Integer.
    def self.grouped(count)
      raise TypeError, "count must be an Integer" unless count.is_a?(Integer)
      raise ArgumentError, "count must be positive" unless count.positive?

      new { |upstream| Pull.grouped(upstream, count) }
    end

    # Creates a running-accumulator flow.
    #
    # The block is called as `block.call(accumulator, element)` for each
    # upstream element, matching `Sink.fold`. The block result becomes the new
    # accumulator and is emitted downstream. The initial accumulator is not
    # emitted before the first upstream element.
    def self.scan(initial, &block)
      raise ArgumentError, "missing block" unless block

      new { |upstream| Pull.scan(upstream, initial, block) }
    end

    # Creates a predicate-based limiting flow.
    #
    # The flow emits leading elements while the block result is truthy. The
    # first false or nil result completes the stream without emitting that
    # element and closes upstream during the same downstream pull. Exceptions
    # raised by the block fail the stream and are re-raised from
    # `Source#run_with`.
    def self.take_while(&block)
      raise ArgumentError, "missing block" unless block

      new { |upstream| Pull.take_while(upstream, block) }
    end

    # Creates a predicate-based prefix-dropping flow.
    #
    # The flow drops leading elements while the block result is truthy. The
    # first false or nil result, and all later elements, pass through unchanged.
    # After that boundary the block is not called again. Exceptions raised by
    # the block fail the stream and are re-raised from `Source#run_with`.
    def self.drop_while(&block)
      raise ArgumentError, "missing block" unless block

      new { |upstream| Pull.drop_while(upstream, block) }
    end

    # Creates a scheduler-backed asynchronous boundary.
    #
    # The boundary starts its producer on the first downstream demand and
    # requires an installed `Fiber.scheduler` at that point. Upstream stages run
    # in a non-blocking producer fiber, downstream stages remain in the caller's
    # current fiber, and each downstream pull resumes at most one upstream pull.
    # Closing the boundary closes upstream and requests producer cancellation.
    # FiberStream does not depend on Async at runtime.
    def self.async
      new { |upstream| Pull.async(upstream) }
    end

    # Creates a bounded asynchronous buffer.
    #
    # The buffer starts its producer on the first downstream demand and requires
    # an installed `Fiber.scheduler` at that point. It preserves element order,
    # stores at most `count` messages, and closes upstream while requesting
    # producer cancellation when closed. `count` must be a positive Integer.
    # FiberStream does not depend on Async at runtime.
    def self.buffer(count)
      raise TypeError, "count must be an Integer" unless count.is_a?(Integer)
      raise ArgumentError, "count must be positive" unless count.positive?

      new { |upstream| Pull.buffer(upstream, count) }
    end

    # Creates a scheduler-aware throttling flow.
    #
    # The `rate:` form creates a fresh `RateLimiter` for each materialization.
    # The `limiter:` form uses the supplied limiter object, which must respond
    # to `acquire(permits:)` and return only after permits are acquired. When
    # FiberStream-owned waiting is required, the current fiber must be
    # non-blocking with an installed `Fiber.scheduler`.
    def self.throttle(**options)
      limiter = build_throttle_limiter(options)

      new { |upstream| Pull.throttle(upstream, limiter.call) }
    end

    # Creates a line-splitting flow.
    #
    # The flow accepts String chunks and emits lines split on "\n". By default
    # it chomps the trailing newline and one preceding "\r". `max_length` is an
    # optional per-line bytesize limit. With `max_length: nil`, one
    # unterminated line can buffer without bound. Set a positive `max_length`
    # for untrusted, network-facing, or otherwise unbounded streams.
    def self.lines(chomp: true, max_length: nil)
      raise TypeError, "chomp must be true or false" unless [true, false].include?(chomp)
      unless max_length.nil? || max_length.is_a?(Integer)
        raise TypeError, "max_length must be nil or an Integer"
      end
      raise ArgumentError, "max_length must be positive" if max_length&.<= 0

      new { |upstream| Pull.lines(upstream, chomp, max_length) }
    end

    # Creates a delimiter-splitting flow.
    #
    # The flow accepts String chunks and emits frames split on the non-empty
    # String `separator`. Separator matching is byte-oriented. By default
    # emitted frames exclude the separator; `keep_separator: true` preserves it
    # on separator-terminated frames. `max_length` is an optional per-frame body
    # bytesize limit. With `max_length: nil`, one unterminated frame can buffer
    # without bound. Set a positive `max_length` for untrusted, network-facing,
    # or otherwise unbounded streams.
    def self.split(separator, keep_separator: false, max_length: nil)
      raise TypeError, "separator must be String" unless separator.is_a?(String)
      raise ArgumentError, "separator must not be empty" if separator.empty?
      raise TypeError, "keep_separator must be true or false" unless [true, false].include?(keep_separator)
      unless max_length.nil? || max_length.is_a?(Integer)
        raise TypeError, "max_length must be nil or an Integer"
      end
      raise ArgumentError, "max_length must be positive" if max_length&.<= 0

      new { |upstream| Pull.split(upstream, separator, keep_separator, max_length) }
    end

    def self.build(&attach) # :nodoc:
      new(&attach)
    end

    def self.build_throttle_limiter(options)
      unknown_keywords = options.keys - [:rate, :per, :burst, :limiter]
      raise ArgumentError, "unknown keywords: #{unknown_keywords.join(", ")}" unless unknown_keywords.empty?

      rate_given = options.key?(:rate)
      per_given = options.key?(:per)
      burst_given = options.key?(:burst)
      limiter_given = options.key?(:limiter)

      if limiter_given
        raise ArgumentError, "cannot pass rate and limiter together" if rate_given
        raise ArgumentError, "cannot pass per with limiter" if per_given
        raise ArgumentError, "cannot pass burst with limiter" if burst_given

        limiter = options.fetch(:limiter)
        raise TypeError, "limiter must respond to acquire" unless limiter.respond_to?(:acquire)

        return -> { limiter }
      end

      raise ArgumentError, "missing rate or limiter" unless rate_given

      rate = options.fetch(:rate)
      per = options.fetch(:per, 1)
      burst = options.fetch(:burst, nil)
      RateLimiter.validate_options!(rate:, per:, burst:)

      -> { RateLimiter.new(rate:, per:, burst:) }
    end

    private_class_method :build_throttle_limiter

    # Returns a reusable flow that applies this flow and then `flow`.
    #
    # Construction is lazy. No upstream stream is attached and no elements are
    # pulled until the composed flow is materialized by a source or sink.
    def via(flow)
      raise TypeError, "expected FiberStream::Flow" unless flow.is_a?(Flow)

      self.class.build do |upstream|
        attached_stream = attach_to(upstream)

        begin
          flow.attach_to(attached_stream)
        rescue StandardError
          begin
            attached_stream.close
          rescue StandardError
            nil
          end
          raise
        end
      end
    end

    # Returns a sink that runs this flow before `sink`.
    #
    # The composed sink accepts this flow's input elements and returns the
    # wrapped sink's materialized value. It closes the attached flow chain after
    # normal completion, failure, or early sink completion.
    def to(sink)
      raise TypeError, "expected FiberStream::Sink" unless sink.is_a?(Sink)

      Sink.build do |stream|
        attached_stream = nil
        primary_error = nil

        begin
          attached_stream = attach_to(stream)
          sink.run_stream(attached_stream)
        rescue StandardError => error
          primary_error = error
          raise
        ensure
          begin
            attached_stream&.close
          rescue StandardError => close_error
            raise close_error unless primary_error
          end
        end
      end
    end

    def initialize(&attach)
      @attach = attach
    end

    private_class_method :new

    def attach_to(upstream) # :nodoc:
      @attach.call(upstream)
    end
  end
end
