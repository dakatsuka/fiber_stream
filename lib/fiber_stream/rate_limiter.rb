# frozen_string_literal: true

module FiberStream
  # Scheduler-aware token-bucket rate limiter.
  #
  # `rate` permits refill every `per` seconds. `burst` is the maximum token
  # capacity and defaults to `rate`. Immediate permit grants do not require a
  # scheduler. When FiberStream must sleep, the current fiber must be
  # non-blocking with an installed `Fiber.scheduler`.
  class RateLimiter
    Request = Data.define(:rate, :per, :burst, :permits, :now)

    class << self
      def validate_options!(rate:, per: 1, burst: nil) # :nodoc:
        rate = validate_rate(rate)
        validate_duration(:per, per)
        validate_burst(burst.nil? ? rate : burst)
        nil
      end

      private

      def validate_rate(rate)
        raise TypeError, "rate must be an Integer" unless rate.is_a?(Integer)
        raise ArgumentError, "rate must be positive" unless rate.positive?

        rate
      end

      def validate_burst(burst)
        raise TypeError, "burst must be an Integer" unless burst.is_a?(Integer)
        raise ArgumentError, "burst must be positive" unless burst.positive?

        burst
      end

      def validate_duration(name, duration)
        raise TypeError, "#{name} must be Numeric" unless duration.is_a?(Numeric)
        raise ArgumentError, "#{name} must be finite and real" unless finite_real?(duration)
        raise ArgumentError, "#{name} must be positive" unless duration.positive?

        duration
      end

      def finite_real?(value)
        return false if value.is_a?(Complex)
        return value.finite? if value.respond_to?(:finite?)

        true
      end
    end

    def initialize(rate:, per: 1, burst: nil, &block)
      self.class.validate_options!(rate:, per:, burst:)

      @rate = rate
      @per = per
      @burst = burst.nil? ? @rate : burst
      @policy = block
      @mutex = Mutex.new
      @tokens = @burst.to_f
      @updated_at = monotonic_now
    end

    # Acquires `permits`, waiting when necessary.
    #
    # Waits are scheduler-backed and non-blocking. Requests larger than `burst`
    # are rejected because the local token bucket could never satisfy them.
    def acquire(permits: 1)
      permits = validate_permits(permits)

      if @policy
        acquire_with_policy(permits)
      else
        acquire_with_token_bucket(permits)
      end

      nil
    end

    private

    def acquire_with_policy(permits)
      loop do
        wait = normalize_policy_wait(@policy.call(request_for(permits)))
        return if wait <= 0

        scheduler_sleep(wait)
      end
    end

    def acquire_with_token_bucket(permits)
      loop do
        wait = nil

        @mutex.synchronize do
          refill_tokens

          if @tokens >= permits
            @tokens -= permits
            return
          end

          wait = (permits - @tokens) / permits_per_second
        end

        scheduler_sleep(wait)
      end
    end

    def request_for(permits)
      Request.new(rate: @rate, per: @per, burst: @burst, permits:, now: monotonic_now)
    end

    def refill_tokens
      now = monotonic_now
      elapsed = now - @updated_at
      @updated_at = now
      @tokens = [@burst, @tokens + (elapsed * permits_per_second)].min
    end

    def permits_per_second
      @rate.to_f / @per.to_f
    end

    def scheduler_sleep(duration)
      validate_scheduler!
      sleep(duration)
    end

    def validate_scheduler!
      return if Fiber.scheduler && !Fiber.current.blocking?

      message =
        if Fiber.scheduler
          "RateLimiter#acquire requires a non-blocking fiber"
        else
          "RateLimiter#acquire requires Fiber.scheduler"
        end
      raise SchedulerRequiredError, message
    end

    def validate_permits(permits)
      raise TypeError, "permits must be an Integer" unless permits.is_a?(Integer)
      raise ArgumentError, "permits must be positive" unless permits.positive?
      raise ArgumentError, "permits must be less than or equal to burst" if permits > @burst

      permits
    end

    def normalize_policy_wait(wait)
      return 0 if wait.nil?
      raise TypeError, "custom wait duration must be Numeric or nil" unless wait.is_a?(Numeric)
      raise ArgumentError, "custom wait duration must be finite and real" unless self.class.send(:finite_real?, wait)

      wait.positive? ? wait : 0
    end

    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
