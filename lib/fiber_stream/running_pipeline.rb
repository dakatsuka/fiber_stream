# frozen_string_literal: true

module FiberStream
  class RunningPipeline
    ValueMessage = Data.define(:value)
    ErrorMessage = Data.define(:error)
    CancelledMessage = Data.define(:error)
    private_constant :ValueMessage, :ErrorMessage, :CancelledMessage

    def initialize(scheduler, &run)
      @scheduler = scheduler
      @completion = nil
      @waiters = []
      @mutex = Mutex.new
      @cancel_requested = false
      @cancellation_error = nil
      @fiber = Fiber.schedule { run_background(run) }
    end

    # Waits for the background pipeline to complete.
    #
    # On success, returns the sink materialized value. On stream failure,
    # re-raises the original exception. If cancellation interrupts the
    # background materialization, raises `PipelineCancelledError`. Waiting
    # before completion requires a scheduler-backed non-blocking fiber; waiting
    # after completion replays the stored result without requiring a scheduler.
    def wait
      message = nil
      waiter = nil

      @mutex.synchronize do
        if @completion
          message = @completion
        else
          validate_scheduler!("RunningPipeline#wait")
          waiter = Thread::Queue.new
          @waiters << waiter
        end
      end

      message ||= waiter.pop
      deliver(message)
    end

    # Requests cancellation of the background pipeline.
    #
    # Cancellation is cooperative and uses the scheduler captured when
    # `Pipeline#run_async` started the background fiber. The method is
    # idempotent. If the captured scheduler cannot interrupt fibers, this
    # method raises `NotImplementedError` without recording a cancellation
    # request.
    def cancel
      fiber = nil
      cancellation_error = nil

      @mutex.synchronize do
        return self if @completion
        return self if @cancel_requested

        unless @scheduler.respond_to?(:fiber_interrupt)
          raise NotImplementedError, "scheduler does not support fiber_interrupt"
        end

        cancellation_error = PipelineCancelledError.new("pipeline cancelled")
        @cancellation_error = cancellation_error
        @cancel_requested = true
        fiber = @fiber
      end

      interrupt(fiber, cancellation_error)
      self
    end

    # Returns true when the background run has completed with success, failure,
    # or cancellation.
    def done?
      @mutex.synchronize { !@completion.nil? }
    end

    # Returns true after `cancel` successfully records a cancellation request.
    def cancel_requested?
      @mutex.synchronize { @cancel_requested }
    end

    private_class_method :new

    private

    def run_background(run)
      complete(ValueMessage.new(value: run.call))
    rescue Exception => error # rubocop:disable Lint/RescueException
      complete(classify_error(error))
    end

    def classify_error(error)
      if cancellation_error?(error)
        CancelledMessage.new(error:)
      else
        ErrorMessage.new(error:)
      end
    end

    def cancellation_error?(error)
      @mutex.synchronize { @cancellation_error.equal?(error) }
    end

    def complete(message)
      waiters = []

      @mutex.synchronize do
        return if @completion

        @completion = message
        waiters = @waiters
        @waiters = []
      end

      waiters.each { |waiter| waiter << message }
    end

    def deliver(message)
      case message
      in ValueMessage[value:]
        value
      in ErrorMessage[error:]
        raise error
      in CancelledMessage[error:]
        raise error
      end
    end

    def interrupt(fiber, cancellation_error)
      return unless fiber&.alive?

      @scheduler.fiber_interrupt(fiber, cancellation_error)
    rescue NotImplementedError, StandardError
      clear_cancellation_request(cancellation_error)
      raise
    end

    def clear_cancellation_request(cancellation_error)
      @mutex.synchronize do
        return unless @cancellation_error.equal?(cancellation_error)
        return if @completion

        @cancellation_error = nil
        @cancel_requested = false
      end
    end

    def validate_scheduler!(operation)
      return if Fiber.scheduler && !Fiber.current.blocking?

      message =
        if Fiber.scheduler
          "#{operation} requires a non-blocking fiber"
        else
          "#{operation} requires Fiber.scheduler"
        end
      raise SchedulerRequiredError, message
    end
  end
end
