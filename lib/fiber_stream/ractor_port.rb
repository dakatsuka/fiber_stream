# frozen_string_literal: true

module FiberStream
  # Typed message envelopes for `Source.ractor_port`.
  #
  # Producers send `Element`, `Complete`, and `Failure` messages to the data
  # port. FiberStream sends `Ack` and `Cancel` messages to the producer-owned
  # acknowledgment port. The envelopes keep stream values distinct from control
  # messages and support Ruby pattern matching.
  module RactorPort
    Element = ::Data.define(:value)
    Complete = ::Data.define
    Failure = ::Data.define(:cause_class_name, :cause_message)
    Ack = ::Data.define
    Cancel = ::Data.define(:reason)
  end
end
