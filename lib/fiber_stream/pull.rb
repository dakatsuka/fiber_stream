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

    def self.map(upstream, transform)
      Map.new(upstream, transform)
    end

    def self.parallel_map(upstream, concurrency, transform)
      ParallelMapBoundary.new(upstream, concurrency, transform)
    end

    def self.select(upstream, predicate)
      Select.new(upstream, predicate)
    end

    def self.take(upstream, count)
      Take.new(upstream, count)
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

    # Pull stream for `Source.io`.
    #
    # Reads are demand-driven: every downstream `#next` performs at most one
    # `readpartial` call. The stream owns IO close only when `close_io` is true,
    # and preserves primary stream failures over cleanup close failures.
    class IOSource
      def initialize(io, chunk_size, close_io)
        @io = io
        @chunk_size = chunk_size
        @close_io = close_io
        @closed = false
        @done = false
        @io_closed = false
      end

      def next
        return DONE if @closed || @done

        validate_scheduler!

        chunk = read_chunk
        return DONE if Pull.done?(chunk)
        return chunk if chunk.is_a?(String)

        fail_with_primary(TypeError.new("readpartial must return a String"))
      end

      def close
        return if @closed

        @closed = true
        @done = true
        close_error = close_io
        raise close_error if close_error
      end

      private

      def validate_scheduler!
        return if Fiber.scheduler && !Fiber.current.blocking?

        message =
          if Fiber.scheduler
            "Source.io requires a non-blocking fiber"
          else
            "Source.io requires Fiber.scheduler"
          end
        fail_with_primary(SchedulerRequiredError.new(message))
      end

      def read_chunk
        @io.readpartial(@chunk_size)
      rescue EOFError
        complete
      rescue StandardError => error
        fail_with_primary(error)
      end

      def complete
        @done = true
        close_error = close_io
        raise close_error if close_error

        DONE
      end

      def fail_with_primary(error)
        @done = true
        close_io
        raise error
      end

      def close_io
        return nil unless @close_io
        return nil if @io_closed

        @io_closed = true
        @io.close
        nil
      rescue StandardError => error
        error
      end
    end

    # Stateless mapping stage.
    #
    # It pulls one upstream element for each downstream demand and applies the
    # transform only to real elements, never to the `DONE` sentinel.
    class Map
      def initialize(upstream, transform)
        @upstream = upstream
        @transform = transform
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        value = @upstream.next
        if Pull.done?(value)
          @done = true
          return DONE
        end

        @transform.call(value)
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end

    # Filtering stage.
    #
    # A single downstream demand may pull multiple upstream elements until the
    # predicate accepts a value or upstream completes. Rejected elements are
    # discarded immediately and are not buffered.
    class Select
      def initialize(upstream, predicate)
        @upstream = upstream
        @predicate = predicate
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        loop do
          value = @upstream.next
          if Pull.done?(value)
            @done = true
            return DONE
          end

          return value if @predicate.call(value)
        end
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end

    # Limiting stage.
    #
    # The stage closes upstream as soon as the limit is reached, including
    # `take(0)` on first demand. This makes early completion visible to
    # resource-owning sources and asynchronous boundaries.
    class Take
      def initialize(upstream, count)
        @upstream = upstream
        @remaining = count
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        if @remaining.zero?
          @done = true
          close
          return DONE
        end

        value = @upstream.next
        if Pull.done?(value)
          @done = true
          return DONE
        end

        @remaining -= 1
        close if @remaining.zero?

        value
      end

      def close
        return if @closed

        @closed = true
        @upstream.close
      end
    end

    # Line-framing stage for `Flow.lines`.
    #
    # The stage keeps an internal byte buffer because line boundaries can cross
    # chunk boundaries. Length checks are per line/frame, not against the
    # aggregate buffer, so already complete valid lines can be emitted before a
    # later over-limit line fails.
    class Lines
      NEWLINE = "\n".b
      CARRIAGE_RETURN = "\r".b

      def initialize(upstream, chomp, max_length)
        @upstream = upstream
        @chomp = chomp
        @max_length = max_length
        @buffer = +"".b
        @closed = false
        @upstream_done = false
      end

      def next
        return DONE if @closed

        loop do
          line = next_buffered_line
          return line if line

          validate_pending_frame_length!
          return complete_from_buffer if @upstream_done

          append_next_chunk
        end
      end

      def close
        return if @closed

        @closed = true
        @buffer.clear
        @upstream.close
      end

      private

      def next_buffered_line
        newline_index = @buffer.index(NEWLINE)
        return nil unless newline_index

        frame = @buffer.slice!(0, newline_index + 1)
        validate_frame_length!(frame)
        format_frame(frame, terminated: true)
      end

      def complete_from_buffer
        return DONE if @buffer.empty?

        frame = @buffer
        @buffer = +"".b
        validate_frame_length!(frame)
        format_frame(frame, terminated: false)
      end

      def append_next_chunk
        chunk = @upstream.next
        if Pull.done?(chunk)
          @upstream_done = true
          return
        end

        unless chunk.is_a?(String)
          raise TypeError, "Flow.lines elements must be String"
        end

        @buffer << chunk.b
        validate_pending_frame_length!
      end

      def validate_pending_frame_length!
        return unless @max_length
        return if @buffer.include?(NEWLINE)
        return if @buffer.bytesize <= @max_length

        fail_frame_too_long
      end

      def validate_frame_length!(frame)
        return unless @max_length
        return if frame.bytesize <= @max_length

        fail_frame_too_long
      end

      def fail_frame_too_long
        @closed = true
        close_upstream
        error = FrameTooLongError.new("frame exceeded max_length #{@max_length}")
        raise error
      end

      def close_upstream
        @upstream.close
        nil
      rescue StandardError => error
        error
      end

      def format_frame(frame, terminated:)
        return frame unless @chomp && terminated

        frame = frame.byteslice(0, frame.bytesize - 1)
        if frame.end_with?(CARRIAGE_RETURN)
          frame = frame.byteslice(0, frame.bytesize - 1)
        end
        frame
      end
    end

    # One-element asynchronous boundary for `Flow.async`.
    #
    # The producer fiber is created lazily on first downstream demand. It
    # advances upstream in a non-blocking fiber and yields one message at a
    # time back to the downstream caller, so it adds an async boundary without
    # adding prefetch.
    class AsyncBoundary
      def initialize(upstream)
        @upstream = upstream
        @producer = nil
        @started = false
        @closed = false
        @done = false
      end

      def next
        return DONE if @closed || @done

        start
        message = @producer.resume

        case message.fetch(0)
        when :value
          message.fetch(1)
        when :done
          complete
        when :error
          @done = true
          raise message.fetch(1)
        end
      end

      def close
        return if @closed

        @closed = true
        @done = true
        @upstream.close
      ensure
        cancel_producer
      end

      private

      def start
        return if @started
        raise SchedulerRequiredError, "Flow.async requires Fiber.scheduler" unless Fiber.scheduler

        @started = true
        @producer = Fiber.new(blocking: false) { run_producer }
      end

      def run_producer
        loop do
          break if @closed

          value = @upstream.next
          if Pull.done?(value)
            Fiber.yield([:done])
            break
          end

          Fiber.yield([:value, value])
        end
      rescue StandardError => exception
        Fiber.yield([:error, exception]) unless @closed
      ensure
        @upstream.close
      end

      def complete
        @done = true
        DONE
      end

      def cancel_producer
        nil
      end
    end

    # Bounded asynchronous prefetch boundary for `Flow.buffer(count)`.
    #
    # The producer task is scheduled lazily and pushes messages into a
    # `Thread::SizedQueue`, so upstream can run ahead only up to the configured
    # queue capacity plus in-flight producer/consumer work. Close is responsible
    # for closing upstream and waking any producer blocked on a full queue.
    class BufferBoundary
      def initialize(upstream, count)
        @upstream = upstream
        @queue = Thread::SizedQueue.new(count)
        @producer = nil
        @started = false
        @closed = false
        @done = false
        @upstream_closed = false
        @upstream_close_error = nil
      end

      def next
        return DONE if @closed || @done

        start
        message = @queue.pop
        return complete if message.nil?

        case message.fetch(0)
        when :value
          message.fetch(1)
        when :done
          complete
        when :error
          @done = true
          raise message.fetch(1)
        end
      end

      def close
        return if @closed

        @closed = true
        @done = true
        close_error = close_upstream
        close_queue
        close_error ||= @upstream_close_error
        raise close_error if close_error
      ensure
        cancel_producer
      end

      private

      def start
        return if @started
        raise SchedulerRequiredError, "Flow.buffer requires Fiber.scheduler" unless Fiber.scheduler

        @started = true
        @producer = Fiber.schedule { run_producer }
      end

      def run_producer
        loop do
          break if @closed

          message = pull_message
          break unless deliver(message)
          break unless message.fetch(0) == :value
        end
      ensure
        @upstream_close_error ||= close_upstream unless @upstream_closed
      end

      def pull_message
        value = @upstream.next
        return terminal_done_message if Pull.done?(value)

        [:value, value]
      rescue StandardError => error
        close_upstream(record_error: false)
        [:error, error]
      end

      def terminal_done_message
        close_error = close_upstream
        close_error ? [:error, close_error] : [:done]
      end

      def deliver(message)
        @queue << message
        true
      rescue ClosedQueueError
        false
      end

      def close_queue
        @queue.close
      end

      def close_upstream(record_error: true)
        return nil if @upstream_closed

        @upstream_closed = true
        @upstream.close
        nil
      rescue StandardError => error
        @upstream_close_error ||= error if record_error
        error
      end

      def complete
        @done = true
        DONE
      end

      def cancel_producer
        nil
      end
    end

    # Ordered scheduler-backed worker boundary for `Flow.parallel_map`.
    #
    # A single dispatcher pulls upstream and assigns sequence numbers while a
    # bounded worker pool maps values. Downstream emits results in input order,
    # so admission is permit-based to keep queued, running, and completed
    # pulled-but-unemitted work bounded by the configured concurrency.
    class ParallelMapBoundary
      TERMINAL_RESULT_CAPACITY = 1
      CancellationError = Class.new(StandardError)

      def initialize(upstream, concurrency, transform)
        @upstream = upstream
        @concurrency = concurrency
        @transform = transform
        @permits = Thread::SizedQueue.new(concurrency)
        @jobs = Thread::SizedQueue.new(concurrency)
        @results = Thread::SizedQueue.new(concurrency + TERMINAL_RESULT_CAPACITY)
        @workers = []
        @dispatcher = nil
        @pending = {}
        @next_sequence = 0
        @next_emit_sequence = 0
        @failure_sequence = nil
        @started = false
        @closed = false
        @done = false
        @admission_closed = false
        @upstream_closed = false
        @upstream_close_error = nil

        concurrency.times { @permits << true }
      end

      def next
        return DONE if @closed || @done

        start
        next_message
      end

      def close
        return if @closed

        @closed = true
        @done = true
        close_error = close_upstream
        close_internal_queues
        close_error ||= @upstream_close_error
        raise close_error if close_error
      ensure
        cancel_fibers
      end

      private

      def start
        return if @started

        validate_scheduler!

        @started = true
        @concurrency.times do
          @workers << Fiber.schedule { run_worker }
        end
        @dispatcher = Fiber.schedule { run_dispatcher }
      end

      def next_message
        loop do
          ready = @pending.delete(@next_emit_sequence)
          if ready
            drain_available_results
            return emit(ready)
          end

          message = @results.pop
          return complete if message.nil?

          record_result(message)
        end
      end

      def emit(message)
        case message.fetch(0)
        when :value
          emit_value(message)
        when :done
          complete
        when :error
          fail_with_ordered_error(message)
        end
      end

      def emit_value(message)
        sequence = message.fetch(1)
        value = message.fetch(2)
        @next_emit_sequence = sequence + 1
        return_permit unless @admission_closed
        value
      end

      def fail_with_ordered_error(message)
        sequence = message.fetch(1)
        error = message.fetch(2)

        if @failure_sequence && sequence > @failure_sequence
          @next_emit_sequence = sequence + 1
          return next_message
        end

        @done = true
        close_result_queue
        cancel_fibers
        raise error
      end

      def complete
        @done = true
        close_result_queue
        DONE
      end

      def run_dispatcher
        loop do
          break if @closed || @admission_closed
          break unless take_permit

          message = pull_job_message
          if message.fetch(0) == :job
            break unless deliver_job(message)
          else
            close_admission(close_upstream: false)
            deliver_result(message)
            break
          end
        end
      rescue CancellationError
        nil
      ensure
        close_upstream unless @upstream_closed || @closed
        close_job_queue
      end

      def pull_job_message
        value = @upstream.next
        return terminal_done_message if Pull.done?(value)

        sequence = @next_sequence
        @next_sequence += 1
        [:job, sequence, value]
      rescue StandardError => error
        close_upstream(record_error: false)
        [:error, @next_sequence, error]
      end

      def terminal_done_message
        close_error = close_upstream
        close_error ? [:error, @next_sequence, close_error] : [:done, @next_sequence]
      end

      def run_worker
        loop do
          break if @closed

          message = @jobs.pop
          break if message.nil?

          deliver_result(map_job(message))
        end
      rescue CancellationError
        nil
      end

      def map_job(message)
        sequence = message.fetch(1)
        value = message.fetch(2)
        [:value, sequence, @transform.call(value)]
      rescue CancellationError
        raise
      rescue StandardError => error
        [:error, sequence, error]
      end

      def record_result(message)
        if message.fetch(0) == :error
          sequence = message.fetch(1)
          @failure_sequence = sequence if @failure_sequence.nil? || sequence < @failure_sequence
          close_admission
        end

        @pending[message.fetch(1)] = message
      end

      def drain_available_results
        loop do
          message = @results.pop(true)
          break if message.nil?

          record_result(message)
        rescue ThreadError
          break
        end
      end

      def close_admission(close_upstream: true)
        return if @admission_closed

        @admission_closed = true
        close_upstream(record_error: false) if close_upstream
        close_permit_queue
        close_job_queue
      end

      def take_permit
        @permits.pop
      rescue ClosedQueueError
        nil
      end

      def return_permit
        @permits << true
      rescue ClosedQueueError
        nil
      end

      def deliver_job(message)
        @jobs << message
        true
      rescue ClosedQueueError
        false
      end

      def deliver_result(message)
        @results << message
        true
      rescue ClosedQueueError
        false
      end

      def close_internal_queues
        close_permit_queue
        close_job_queue
        close_result_queue
      end

      def close_permit_queue
        @permits.close
      end

      def close_job_queue
        @jobs.close
      end

      def close_result_queue
        @results.close
      end

      def close_upstream(record_error: true)
        return nil if @upstream_closed

        @upstream_closed = true
        @upstream.close
        nil
      rescue StandardError => error
        @upstream_close_error ||= error if record_error
        error
      end

      def cancel_fibers
        scheduler = Fiber.scheduler
        return unless scheduler.respond_to?(:fiber_interrupt)

        (@workers + [@dispatcher]).compact.each do |fiber|
          next unless fiber.alive?

          scheduler.fiber_interrupt(fiber, CancellationError.new)
        rescue StandardError
          nil
        end
      end

      def validate_scheduler!
        return if Fiber.scheduler && !Fiber.current.blocking?

        message =
          if Fiber.scheduler
            "Flow.parallel_map requires a non-blocking fiber"
          else
            "Flow.parallel_map requires Fiber.scheduler"
          end
        raise SchedulerRequiredError, message
      end
    end

    private_constant :DONE, :Each, :IOSource, :Map, :Select, :Take, :Lines,
                     :AsyncBoundary, :BufferBoundary, :ParallelMapBoundary
  end
end
