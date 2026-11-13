require 'spec_helper'

describe 'Batched reservation (Delayed::Backend::Mongoid.batch_reserve)' do
  # Records the names of the commands sent to MongoDB (a command monitoring
  # subscriber). Scoped to this spec, not defined at the top level.
  command_counter = Struct.new(:commands, :mutex) do
    def started(event)
      mutex.synchronize { commands << event.command_name }
    end

    def succeeded(_event); end

    def failed(_event); end
  end

  let(:job_class) { Delayed::Backend::Mongoid::Job }
  let(:workers) { Array.new(4) { |i| Delayed::Worker.new.tap { |w| w.thread_index = i } } }
  let(:counter) { command_counter.new([], Mutex.new) }

  around do |example|
    Delayed::Backend::Mongoid.batch_reserve = true
    example.run
  ensure
    Delayed::Backend::Mongoid.reset_configuration!
    Delayed::Worker.reset
    # Not reset by Worker.reset:
    Delayed::Worker.exit_on_complete = false
    Delayed::Worker.min_priority = nil
    Delayed::Worker.max_priority = nil
  end

  def enqueue(count, options = {})
    Array.new(count) { |i| Delayed::Job.enqueue(SimpleJob.new, { priority: i }.merge(options)) }
  end

  # The commands reserve_batch sends to MongoDB.
  def commands_of
    client = Mongoid.default_client
    client.subscribe(Mongo::Monitoring::COMMAND, counter)
    yield
    counter.commands
  ensure
    client.send(:monitoring).subscribers[Mongo::Monitoring::COMMAND].delete(counter)
  end

  describe 'the command counter' do
    it 'is not defined at the top level' do
      expect(Object.const_defined?(:CommandCounter)).to be false
    end

    it 'records the commands sent while counting, in order' do
      enqueue(1)
      commands = commands_of do
        job_class.collection.find.to_a
        job_class.collection.insert_one(x: 1)
      end
      expect(commands).to eq %w[find insert]
    end

    it 'stops counting once done' do
      commands_of { job_class.collection.find.to_a }
      job_class.collection.find.to_a
      expect(counter.commands).to eq %w[find]
      expect(Mongoid.default_client.send(:monitoring).subscribers[Mongo::Monitoring::COMMAND]).not_to include counter
    end

    it 'stops counting when the block raises' do
      expect { commands_of { raise IOError } }.to raise_error(IOError)
      expect(Mongoid.default_client.send(:monitoring).subscribers[Mongo::Monitoring::COMMAND]).not_to include counter
    end

    it 'counts commands sent from several threads' do
      commands = commands_of { Array.new(4) { Thread.new { job_class.collection.find.to_a } }.each(&:join) }
      expect(commands).to eq %w[find] * 4
    end
  end

  describe '.partial_batch_may_leave_jobs?' do
    it 'is true with batch_reserve, as candidates may be lost to other processes' do
      expect(job_class.partial_batch_may_leave_jobs?).to be true
    end

    it 'is false without, as reserve stops only when no job is left' do
      Delayed::Backend::Mongoid.batch_reserve = false
      expect(job_class.partial_batch_may_leave_jobs?).to be false
    end
  end

  it 'is off by default' do
    Delayed::Backend::Mongoid.batch_reserve = false
    expect(Delayed::Backend::Mongoid.batch_reserve).to be false
  end

  it 'locks a distinct job under each worker name, in 3 round trips' do
    enqueue(6)
    reserved = nil
    commands = commands_of { reserved = job_class.reserve_batch(workers) }

    expect(commands).to eq %w[find update find]
    expect(reserved.map(&:first)).to eq workers
    expect(reserved.map { |_, job| job.id }.uniq.size).to eq 4
    reserved.each do |worker, job|
      expect(job.locked_by).to eq worker.name
      expect(job.locked_at).to be_within(1).of(Time.now.utc)
      expect(job_class.find(job.id).locked_by).to eq worker.name
    end
  end

  it 'takes the same number of round trips for 32 workers' do
    many = Array.new(32) { |i| Delayed::Worker.new.tap { |w| w.thread_index = i } }
    enqueue(32)
    reserved = nil
    commands = commands_of { reserved = job_class.reserve_batch(many) }
    expect(commands).to eq %w[find update find]
    expect(reserved.size).to eq 32
  end

  it 'reserves the jobs reserve would, in priority order' do
    jobs = enqueue(6)
    reserved = job_class.reserve_batch(workers)
    expect(reserved.map { |_, job| job.id }).to eq jobs.first(4).map(&:id)
  end

  it 'reserves as many jobs as there are, fewer than workers' do
    jobs = enqueue(2)
    reserved = job_class.reserve_batch(workers)
    expect(reserved.map(&:first)).to eq workers.first(2)
    expect(reserved.map { |_, job| job.id }).to match_array jobs.map(&:id)
  end

  it 'returns no jobs in one round trip when none is available' do
    reserved = nil
    commands = commands_of { reserved = job_class.reserve_batch(workers) }
    expect(reserved).to eq []
    expect(commands).to eq %w[find]
  end

  it 'honours queues and priorities' do
    Delayed::Worker.queues = %w[mail]
    Delayed::Worker.min_priority = 1
    Delayed::Worker.max_priority = 5
    wanted = Delayed::Job.enqueue(SimpleJob.new, queue: 'mail', priority: 3)
    Delayed::Job.enqueue(SimpleJob.new, queue: 'other', priority: 3)
    Delayed::Job.enqueue(SimpleJob.new, queue: 'mail', priority: 0)
    Delayed::Job.enqueue(SimpleJob.new, queue: 'mail', priority: 9)
    expect(job_class.reserve_batch(workers).map { |_, job| job.id }).to eq [wanted.id]
  end

  it 'skips jobs scheduled in the future, and failed jobs' do
    Delayed::Job.enqueue(SimpleJob.new, run_at: 1.hour.from_now)
    enqueue(1).first.update_attributes!(failed_at: Time.now.utc)
    expect(job_class.reserve_batch(workers)).to eq []
  end

  it 'skips jobs freshly locked by other workers, and takes over expired locks' do
    fresh, expired = enqueue(2)
    fresh.update_attributes!(locked_by: 'other', locked_at: Time.now.utc)
    expired.update_attributes!(locked_by: 'other', locked_at: 2.hours.ago)
    reserved = job_class.reserve_batch(workers, 1.hour)
    expect(reserved.map { |_, job| job.id }).to eq [expired.id]
    expect(job_class.find(fresh.id).locked_by).to eq 'other'
  end

  it 'takes back jobs left locked under the names of the (idle) workers' do
    leftover, = enqueue(1)
    leftover.update_attributes!(locked_by: workers[2].name, locked_at: Time.now.utc)
    reserved = job_class.reserve_batch(workers)
    expect(reserved.size).to eq 1
    worker, job = reserved.first
    expect(job.id).to eq leftover.id
    expect(job.locked_by).to eq worker.name
  end

  it 'does not take jobs locked by sibling workers that are busy' do
    busy, = enqueue(1)
    sibling = Delayed::Worker.new.tap { |w| w.thread_index = 9 }
    busy.update_attributes!(locked_by: sibling.name, locked_at: Time.now.utc)
    expect(job_class.reserve_batch(workers)).to eq []
  end

  it 'leaves candidates that another process reserved meanwhile to it' do
    jobs = enqueue(4)
    allow_any_instance_of(Mongo::Collection).to receive(:bulk_write).and_wrap_original do |m, claims, **options|
      # Another process locks the second candidate between our find and our update.
      job_class.where(_id: jobs[1].id).update_all('$set' => { locked_by: 'other process', locked_at: Time.now.utc })
      m.call(claims, **options)
    end
    reserved = job_class.reserve_batch(workers)

    expect(reserved.map(&:first)).to eq [workers[0], workers[2], workers[3]]
    expect(reserved.map { |_, job| job.id }).to eq [jobs[0], jobs[2], jobs[3]].map(&:id)
    expect(job_class.find(jobs[1].id).locked_by).to eq 'other process'
  end

  it 'batches as soon as two workers are idle' do
    enqueue(2)
    expect(job_class).not_to receive(:reserve)
    reserved = nil
    commands = commands_of { reserved = job_class.reserve_batch(workers.first(2)) }
    expect(commands).to eq %w[find update find]
    expect(reserved.size).to eq 2
  end

  it 'reserves for a single idle worker in one round trip' do
    enqueue(2)
    reserved = nil
    commands = commands_of { reserved = job_class.reserve_batch(workers.first(1)) }
    expect(commands).to eq %w[findAndModify]
    expect(reserved.map(&:first)).to eq workers.first(1)
  end

  it 'reserves one job per worker when off' do
    Delayed::Backend::Mongoid.batch_reserve = false
    enqueue(4)
    reserved = nil
    commands = commands_of { reserved = job_class.reserve_batch(workers) }
    expect(commands).to eq %w[findAndModify] * 4
    expect(reserved.size).to eq 4
  end

  # Several "processes" (threads with their own worker names) reserving from
  # the same collection at once.
  it 'never locks a job for two workers under contention' do
    jobs = enqueue(60)
    groups = Array.new(3) do |g|
      Array.new(8) { |i| Delayed::Worker.new.tap { |w| w.name = "process:#{g} thread:#{i}" } }
    end
    reserved = Queue.new
    threads = groups.map do |group|
      Thread.new do
        loop do
          batch = job_class.reserve_batch(group)
          break if batch.empty? && job_class.where(locked_by: nil).none?

          batch.each do |worker, job|
            reserved << [worker.name, job.id]
            # Release the worker (keep its job locked under another name).
            job_class.where(_id: job.id).update_all('$set' => { locked_by: "done #{job.id}" })
          end
        end
      end
    end
    Timeout.timeout(30) { threads.each(&:join) }

    ids = Array.new(reserved.size) { reserved.pop.last }
    expect(ids.size).to eq 60
    expect(ids).to match_array jobs.map(&:id)
  end

  describe 'with the dispatcher' do
    it 'runs every job exactly once across two dispatchers, with partial batches re-polled at once' do
      ConcurrencyJob.reset
      40.times { |i| Delayed::Job.enqueue(ConcurrencyJob.new(i)) }
      Delayed::Worker.exit_on_complete = true
      dispatchers = Array.new(2) do |i|
        Delayed::Dispatcher.new(threads: 6, sleep_delay: 5).tap { |d| d.name_prefix = "dj.#{i} " }
      end
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      Timeout.timeout(30) { dispatchers.map { |d| Thread.new { d.run } }.each(&:join) }
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

      expect(ConcurrencyJob.runs.keys.sort).to eq((0...40).to_a)
      expect(ConcurrencyJob.runs.values).to all(eq 1)
      expect(Delayed::Job.count).to eq 0
      # Never slept sleep_delay (5s) while jobs were left.
      expect(elapsed).to be < 5
    end
  end
end
