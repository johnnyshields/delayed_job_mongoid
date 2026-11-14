require 'mongo'
require 'delayed/plugin'

module Delayed
  module Plugins
    # Silences the Mongo logger except when performing jobs.
    # Requires the Mongoid/Mongo loggers to be instances of ActiveSupport::Logger.
    #
    # The silenced level is thread-local, so it is applied on every thread
    # that talks to the job collection: worker threads (:execute) and, with
    # a multi-threaded delayed_job, the dispatcher thread that reserves jobs
    # (:loop wraps each reservation poll).
    class MongoSilencer < Delayed::Plugin
      callbacks do |lifecycle|
        %i[execute loop].each do |event|
          lifecycle.before(event) do |*_args|
            store_log_level
            silence!
          end
        end

        # As of Delayed Job 4.10, the :perform hook includes the
        # operations for job deletion and job errors, so these will
        # still appear in the log.
        lifecycle.around(:perform) do |*_args, &block|
          unsilence!
          block.call
        ensure
          silence!
        end
      end

      class << self
        # Stores, for this thread, the logger's own level, ignoring any
        # thread-local level (e.g. one set by #silence! on this thread). Kept
        # per thread like the level it restores: worker threads call it
        # concurrently.
        def store_log_level
          Thread.current[:delayed_mongo_log_level] = ::Logger.instance_method(:level).bind_call(Mongo::Logger.logger) if mongo_logger?
        end

        def silence!
          Mongo::Logger.logger.local_level = Logger::WARN if stored_log_level && mongo_logger?
        end

        def unsilence!
          Mongo::Logger.logger.local_level = stored_log_level if stored_log_level && mongo_logger?
        end

        def stored_log_level
          Thread.current[:delayed_mongo_log_level]
        end

        private

        def mongo_logger?
          Mongo::Logger.logger.respond_to?(:local_level)
        end
      end
    end
  end
end
