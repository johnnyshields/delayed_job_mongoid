require 'forwardable'

module Delayed
  module Backend
    module Mongoid
      # Settings of the Mongoid backend, set in an initializer:
      #
      #   Delayed::Backend::Mongoid.configure do |config|
      #     config.batch_reserve = true
      #   end
      #
      # or directly, e.g. Delayed::Backend::Mongoid.batch_reserve = true.
      class Configuration
        # Batched reservation (off by default): when several threads are
        # idle, Job.reserve_batch reserves their jobs in 3 round trips
        # instead of one find_one_and_update per thread.
        attr_reader :batch_reserve

        def initialize
          @batch_reserve = false
        end

        def batch_reserve=(value)
          @batch_reserve = value ? true : false
        end
      end

      class << self
        extend Forwardable

        def_delegators :configuration, :batch_reserve, :batch_reserve=

        def configuration
          @configuration ||= Configuration.new
        end

        # Yields the configuration; returns it.
        def configure
          yield configuration
          configuration
        end

        # Restores the default settings (e.g. between specs).
        def reset_configuration!
          @configuration = Configuration.new
        end
      end
    end
  end
end
