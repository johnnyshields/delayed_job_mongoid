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

          # Deletes the job only while the named worker holds its lock.
          def destroy_if_locked_by(worker_name) # rubocop:disable Naming/PredicateMethod
            self.class.where(_id: id, locked_by: worker_name).delete.positive?
          end

          # Saves the job only while the named worker holds its lock.
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

          # MongoDB 8.0
          SORTED_UPDATE_WIRE_VERSION = 25
          # The server's error for an update with a sort before MongoDB 8.0
          UNKNOWN_FIELD = 40_415
          DRIVER_SORTS_UPDATES = ::Mongo::BulkWrite::Transformable::UPDATE_ONE_TRANSFORM
                                 .call(filter: {}, update: {}, sort: { _id: 1 }).key?('sort')
          private_constant :SORTED_UPDATE_WIRE_VERSION, :UNKNOWN_FIELD, :DRIVER_SORTS_UPDATES

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

          # Reserves a job for each of the given (idle) workers, or as many as
          # are available; returns [[worker, job], ...]. With batch_reserve
          # (see Delayed::Backend::Mongoid::Configuration) and more than one
          # worker, takes a fixed number of round trips however many workers
          # there are; otherwise one find_one_and_update per worker (see
          # reserve).
          #
          # On MongoDB 8.0+ (which can sort an updateOne), 2 round trips:
          #
          # 1. one bulk write of sorted updateOnes, each locking the first job
          #    reserve would pick under one worker's name (the server picks
          #    and locks each job atomically, so none is lost to other
          #    processes: a short batch means no job is left);
          # 2. read back the locked jobs.
          #
          # On older servers, 3 round trips:
          #
          # 1. find up to one candidate per worker, as reserve would;
          # 2. one bulk write of conditional updates, each locking one
          #    candidate under one worker's name if it is still available
          #    (another process may have reserved it meanwhile);
          # 3. read back the candidates claimed under our names.
          #
          # Candidates lost to other processes are not retried there: fewer
          # jobs than workers are returned, and delayed_job polls again at
          # once.
          def reserve_batch(workers, max_run_time = Worker.max_run_time)
            # A single worker takes one find_one_and_update (see reserve).
            return super unless Delayed::Backend::Mongoid.batch_reserve && workers.size > 1

            right_now = db_time_now
            return reserve_batch_by_candidates(workers, right_now, max_run_time) unless sorted_updates?

            begin
              reserve_batch_sorted(workers, right_now, max_run_time)
            rescue ::Mongo::Error::OperationFailure => e
              # The primary changed to a pre-8.0 server since we checked.
              raise unless e.code == UNKNOWN_FIELD

              reserve_batch_by_candidates(workers, right_now, max_run_time)
            end
          end

          # A batch may come back short while jobs are left only when it
          # reserves candidates found beforehand (batch_reserve before MongoDB
          # 8.0): delayed_job then polls again at once.
          def partial_batch_may_leave_jobs?
            Delayed::Backend::Mongoid.batch_reserve && !sorted_updates?
          end

          # Whether updateOne can take a sort: MongoDB 8.0+ (whatever the
          # feature compatibility version), with a driver that sends it
          # (older ones drop it silently, which would ignore priorities).
          def sorted_updates?
            DRIVER_SORTS_UPDATES &&
              collection.client.cluster.next_primary.description.max_wire_version >= SORTED_UPDATE_WIRE_VERSION
          end

          # Lock heartbeats: delayed_job refreshes the locks of running jobs,
          # and reserve takes over locks older than the expiry it is given.
          def lock_heartbeats?
            true
          end

          def touch_locks(worker_names)
            any_in(locked_by: worker_names).update_all('$set' => { locked_at: db_time_now }).modified_count
          end

          # With Delayed::Worker.silence_polling, silences the Mongo driver's
          # logger below WARN on the polling thread. Needs an
          # ActiveSupport::Logger (thread-local levels); other loggers log.
          def silence_polling(&)
            logger = ::Mongo::Logger.logger
            return yield unless logger.respond_to?(:silence)

            logger.silence(::Logger::WARN, &)
          end

          # The jobs locked by the named worker, so that delayed_job can
          # recover them when the worker's process dies.
          def locked_jobs(worker_name)
            where(locked_by: worker_name).to_a
          end

          # When a worker is exiting, make sure we don't have any locked jobs.
          def clear_locks!(worker_name)
            where(locked_by: worker_name).update_all(locked_at: nil, locked_by: nil)
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

          def reserve_batch_sorted(workers, right_now, max_run_time)
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

          def reserve_batch_by_candidates(workers, right_now, max_run_time)
            names = workers.map(&:name)
            candidates = batch_reservation_criteria(names, right_now, max_run_time).limit(workers.size).pluck(:_id)
            return [] if candidates.empty?

            claims = candidates.zip(workers).map do |id, worker|
              {
                update_one: {
                  filter: batch_reservation_selector(names, right_now, max_run_time).merge('_id' => id),
                  update: { '$set' => { 'locked_at' => right_now, 'locked_by' => worker.name } }
                }
              }
            end
            collection.bulk_write(claims, ordered: false)

            claimed_jobs(workers, right_now)
          end

          # The jobs just locked (at right_now) under the workers' names, by
          # worker. Other jobs left locked under those names (leftovers from
          # a crash that weren't re-locked) stay for a later reservation.
          def claimed_jobs(workers, right_now)
            claimed = where(locked_at: right_now).any_in(locked_by: workers.map(&:name)).index_by(&:locked_by)
            workers.filter_map { |worker| (job = claimed[worker.name]) && [worker, job] }
          end

          # The jobs any of the named workers can reserve: available ones, and
          # ones left locked under one of their names (they are idle, so
          # those locks are leftovers). Sorted as in reservation_criteria.
          def batch_reservation_criteria(names, right_now, max_run_time)
            criteria = where(batch_reservation_selector(names, right_now, max_run_time))
                       .reorder(locked_by: -1, priority: 1, run_at: 1)
            criteria = criteria.gte(priority: Worker.min_priority.to_i) if Worker.min_priority
            criteria = criteria.lte(priority: Worker.max_priority.to_i) if Worker.max_priority
            criteria = criteria.any_in(queue: Worker.queues) if Worker.queues.any?
            criteria
          end

          # Selector of a job that is (still) reservable by the named workers.
          def batch_reservation_selector(names, right_now, max_run_time)
            {
              'run_at' => { '$lte' => right_now },
              'failed_at' => nil,
              '$or' => [
                { 'locked_by' => { '$in' => names } },
                { 'locked_at' => { '$not' => { '$gte' => right_now - max_run_time } } }
              ]
            }
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
