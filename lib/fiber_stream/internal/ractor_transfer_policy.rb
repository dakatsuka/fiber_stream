# frozen_string_literal: true

module FiberStream
  module Internal # :nodoc:
    module RactorTransferPolicy # :nodoc:
      module_function

      def validate!(name, value)
        return if [:copy, :move].include?(value)

        raise ArgumentError, "#{name} must be :copy or :move"
      end
    end
  end

  private_constant :Internal
end
