require 'forwardable'

module Delayed
  module Backend
    module Mongoid
      # Mongoid backend settings:
      #
      #   Delayed::Backend::Mongoid.configure do |config|
      #     config.batch_reserve = true
      #   end
      class Configuration
        # Reserve jobs for several idle threads in 2 round trips instead of
        # one find_one_and_update each. Default: false.
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

        # Set eagerly: a lazy `||=` could create two under concurrent threads.
        attr_reader :configuration

        def configure
          yield configuration
          configuration
        end

        def reset_configuration!
          @configuration = Configuration.new
        end
      end

      reset_configuration!
    end
  end
end
