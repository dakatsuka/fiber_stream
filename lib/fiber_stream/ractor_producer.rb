# frozen_string_literal: true

module FiberStream
  # Producer-side context for `Source.ractor_producer`.
  #
  # Producer blocks call `emit`, `complete`, or `fail` to send one protocol
  # message after receiving one downstream acknowledgment. A `false` return
  # means cooperative cancellation was observed before the requested message
  # could be sent.
  class RactorProducer
    def initialize(data_port, ack_port, transfer)
      @data_port = data_port
      @ack_port = ack_port
      @transfer = transfer
      @terminal = false
      @cancelled = false
      @send_failed = false
    end

    def emit(value, transfer: nil)
      return false if terminal? || cancelled?

      message_transfer = validate_transfer_override!(transfer)
      return false unless wait_for_ack

      send_emitted_message(RactorPort::Element.new(value), message_transfer)
    end

    def complete
      return false if terminal? || cancelled?
      return false unless wait_for_ack

      send_terminal_message(RactorPort::Complete.new)
    end

    def fail(error = nil, cause_class_name: nil, cause_message: nil)
      return false if terminal? || cancelled?

      failure = failure_message(error, cause_class_name, cause_message)
      return false unless wait_for_ack

      send_terminal_message(failure)
    end

    def cancelled?
      @cancelled
    end

    private

    attr_reader :terminal
    alias terminal? terminal
    attr_reader :send_failed
    alias send_failed? send_failed

    def send_emitted_message(message, transfer)
      send_data_message(message, transfer)
      true
    rescue Exception => error # rubocop:disable Lint/RescueException
      report_same_ack_failure(error)
      false
    end

    def send_terminal_message(message)
      send_data_message(message, @transfer)
      @terminal = true
      true
    rescue Exception => send_error # rubocop:disable Lint/RescueException
      report_same_ack_failure(send_error)
      false
    end

    def validate_transfer_override!(transfer)
      return @transfer if transfer.nil?
      return transfer if [:copy, :move].include?(transfer)

      raise ArgumentError, "transfer must be :copy or :move"
    end

    def wait_for_ack
      case @ack_port.receive
      in RactorPort::Ack
        true
      in RactorPort::Cancel
        @cancelled = true
        false
      else
        raise TypeError, "invalid ractor producer control message"
      end
    end

    def send_data_message(message, transfer)
      if transfer == :move
        @data_port.send(message, move: true)
      else
        @data_port.send(message)
      end
    end

    def failure_message(error, cause_class_name, cause_message)
      if error
        return RactorPort::Failure.new(safe_class_name(error), safe_message(error))
      end

      unless cause_class_name.is_a?(String) && cause_message.is_a?(String)
        raise ArgumentError, "fail requires an error or String failure metadata"
      end

      RactorPort::Failure.new(cause_class_name, cause_message)
    end

    def safe_class_name(error)
      name = error.class.name
      name.is_a?(String) && !name.empty? ? name : "Exception"
    rescue Exception # rubocop:disable Lint/RescueException
      "Exception"
    end

    def safe_message(error)
      message = error.message
      message.is_a?(String) ? message : ""
    rescue Exception # rubocop:disable Lint/RescueException
      ""
    end

    def report_same_ack_failure(error)
      send_data_message(RactorPort::Failure.new(safe_class_name(error), safe_message(error)), :copy)
      @terminal = true
    rescue Exception # rubocop:disable Lint/RescueException
      @terminal = true
      @send_failed = true
    end
  end

  # Builder passed to `Source.ractor_merge_producers`.
  #
  # Each `producer` call records one lazily started owned producer definition.
  # Registration validates producer block isolation and transfer policy but
  # does not create Ractor ports or start producer code.
  class RactorProducerGroup
    Definition = Data.define(:args, :transfer, :block)
    private_constant :Definition

    def initialize(default_transfer)
      @default_transfer = default_transfer
      @definitions = []
    end

    def producer(*args, transfer: nil, &block)
      raise ArgumentError, "missing block" unless block
      unless transfer.nil? || [:copy, :move].include?(transfer)
        raise ArgumentError, "transfer must be :copy or :move"
      end
      raise TypeError, "block must be shareable" unless Ractor.shareable?(block)

      @definitions << Definition.new(args:, transfer: transfer || @default_transfer, block:)
      self
    end

    def definitions
      @definitions.dup.freeze
    end
  end
end
