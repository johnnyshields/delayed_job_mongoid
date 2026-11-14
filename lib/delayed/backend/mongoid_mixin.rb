require 'active_support/concern'
require 'delayed_job'
require 'mongoid'
require 'delayed/backend/mongoid_configuration'

module Delayed
  module Backend
    module Mongoid
      module Mixin
        extend ::ActiveSupport::Concern

        included do
          include ::Delayed::Backend::Base
          include ::Mongoid::Document
          include ::Mongoid::Timestamps

          field :priority,    type: Integer, default: 0
          field :attempts,    type: Integer, default: 0
          field :handler,     type: String
          field :run_at,      type: Time
          field :locked_at,   type: Time
          field :locked_by,   type: String
          field :failed_at,   type: Time
          field :last_error,  type: String
          field :queue,       type: String

          index locked_by: -1, priority: 1, run_at: 1
          index queue: 1, locked_by: -1, priority: 1, run_at: 1
          index failed_at: 1, locked_by: -1, priority: 1, run_at: 1
          index failed_at: 1, queue: 1, locked_by: -1, priority: 1, run_at: 1

          before_save :set_default_run_at

          def reload(*args)
            reset
            super
          end

          def destroy_if_locked_by(worker_name) # rubocop:disable Naming/PredicateMethod
            self.class.where(_id: id, locked_by: worker_name).delete.positive?
          end

          def save_if_locked_by(worker_name)
            run_callbacks(:save) do
              changes = changed_attributes.keys.to_h { |field| [field, attributes[field]] }
              changes['updated_at'] = self.updated_at = Time.now.utc
              updated = self.class.where(_id: id, locked_by: worker_name).update_all('$set' => changes).modified_count
              move_changes if updated.positive?
              updated.positive?
            end
          end
        end

        module ClassMethods
          include ::Delayed::Backend::Base::ClassMethods

          def db_time_now
            Time.now.utc
          end

          # Reserves one job for the worker.
          # Atomically picks and locks one job from the collection.
          def reserve(worker, max_run_time = Worker.max_run_time)
            right_now = db_time_now
            criteria = reservation_criteria(worker, right_now, max_run_time)
            criteria.find_one_and_update(
              { '$set' => { locked_at: right_now, locked_by: worker.name } },
              { return_document: :after }
            )
          end

          # With batch_reserve and several workers: one bulk write of sorted
          # updateOnes, each atomically locking one job (so other processes
          # can't take it; a short batch means no jobs are left), then one
          # read-back of the locked jobs.
          def reserve_batch(workers, max_run_time = Worker.max_run_time)
            return super unless Delayed::Backend::Mongoid.batch_reserve && workers.size > 1

            right_now = db_time_now
            claims = workers.map do |worker|
              criteria = reservation_criteria(worker, right_now, max_run_time)
              {
                update_one: {
                  filter: criteria.selector,
                  update: { '$set' => { 'locked_at' => right_now, 'locked_by' => worker.name } },
                  # Requires MongoDB Server 8.0+ (sort on updateOne)
                  sort: criteria.options[:sort]
                }
              }
            end
            return [] if collection.bulk_write(claims, ordered: false).modified_count.zero?

            claimed_jobs(workers, right_now)
          end

          # Running jobs' locks are refreshed; reserve takes over expired ones.
          def lock_heartbeats?
            true
          end

          def touch_locks(worker_names)
            any_in(locked_by: worker_names).update_all('$set' => { locked_at: db_time_now }).modified_count
          end

          # Needs ActiveSupport::Logger, whose silence level is thread-local.
          def silence_polling(&)
            logger = ::Mongo::Logger.logger
            return yield unless logger.respond_to?(:silence)

            logger.silence(::Logger::WARN, &)
          end

          def locked_jobs(worker_name)
            where(locked_by: worker_name).to_a
          end

          # When a worker is exiting, make sure we don't have any locked jobs.
          def clear_locks!(worker_name)
            where(locked_by: worker_name).update_all(locked_at: nil, locked_by: nil)
          end

          def locked_jobs_of(worker_names)
            any_in(locked_by: worker_names).to_a
          end

          def clear_locks_of!(worker_names)
            any_in(locked_by: worker_names).update_all(locked_at: nil, locked_by: nil)
          end

          # In a multi-process setup, this will be called at boot time
          # to close unnecessary database connections on the parent process.
          def before_fork
            ::Mongoid.disconnect_clients
          end

          # In a multi-process setup, this will be called to ensure fresh
          # database connections are immediately made on each newly spawned child process.
          def after_fork
            ::Mongoid::Clients.clients.each_value do |client|
              client.close
              client.reconnect
            end
          end

          private

          # Older locks under these names (crash leftovers) are left for a later reserve.
          def claimed_jobs(workers, right_now)
            claimed = where(locked_at: right_now).any_in(locked_by: workers.map(&:name)).index_by(&:locked_by)
            workers.filter_map { |worker| (job = claimed[worker.name]) && [worker, job] }
          end

          # Mongo criteria matching all the jobs the worker can reserve.
          # Jobs are sorted by priority and run_at.
          def reservation_criteria(worker, right_now, max_run_time)
            criteria = where(
              run_at: { '$lte' => right_now },
              failed_at: nil
            ).any_of(
              { locked_by: worker.name },
              { locked_at: { '$not' => { '$gte' => (right_now - max_run_time) } } }
            ).reorder(locked_by: -1, priority: 1, run_at: 1)

            criteria = criteria.gte(priority: Worker.min_priority.to_i) if Worker.min_priority
            criteria = criteria.lte(priority: Worker.max_priority.to_i) if Worker.max_priority
            criteria = criteria.any_in(queue: Worker.queues) if Worker.queues.any?
            criteria
          end
        end
      end
    end
  end
end
