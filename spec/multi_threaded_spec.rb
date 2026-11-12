require 'spec_helper'

# Records how many jobs ran at once and how often each job ran.
class ConcurrencyJob
  @mutex = Mutex.new

  class << self
    attr_reader :mutex, :runs, :max_running

    def reset
      @running = 0
      @max_running = 0
      @runs = Hash.new(0)
    end

    def started(key)
      mutex.synchronize do
        @runs[key] += 1
        @running += 1
        @max_running = [@max_running, @running].max
      end
    end

    def finished
      mutex.synchronize { @running -= 1 }
    end
  end

  attr_reader :key

  def initialize(key)
    @key = key
  end

  def perform
    self.class.started(key)
    sleep 0.05
  ensure
    self.class.finished
  end
end

describe 'multi-threaded delayed_job with Mongoid' do
  let(:workers) { Array.new(3) { |i| Delayed::Worker.new.tap { |w| w.thread_index = i } } }

  before { ConcurrencyJob.reset }

  describe '.reserve_batch' do
    it 'locks a distinct job under each thread name' do
      3.times { |i| Delayed::Job.enqueue(ConcurrencyJob.new(i)) }
      reserved = Delayed::Job.reserve_batch(workers)

      expect(reserved.map(&:first)).to eq workers
      expect(reserved.map { |_, job| job.id }.uniq.size).to eq 3
      reserved.each do |worker, job|
        expect(job.locked_by).to eq worker.name
        expect(Delayed::Job.find(job.id).locked_by).to eq worker.name
      end
    end

    it 'stops at the first thread without a job' do
      Delayed::Job.enqueue(ConcurrencyJob.new(0))
      expect(Delayed::Job).to receive(:reserve).twice.and_call_original
      expect(Delayed::Job.reserve_batch(workers).size).to eq 1
    end
  end

  describe '.reserve' do
    it 'does not take a job locked by a sibling thread' do
      Delayed::Job.enqueue(ConcurrencyJob.new(0))
      job = Delayed::Job.reserve(workers[0])

      expect(job).not_to be_nil
      expect(Delayed::Job.reserve(workers[1])).to be_nil
      expect(Delayed::Job.reserve(workers[0]).id).to eq job.id
    end
  end

  describe 'Delayed::Dispatcher' do
    let(:dispatcher) { Delayed::Dispatcher.new(threads: 4, sleep_delay: 0.1) }

    around do |example|
      Delayed::Worker.exit_on_complete = true
      example.run
    ensure
      Delayed::Worker.reset
      Delayed::Worker.exit_on_complete = false
    end

    it 'runs every job exactly once, concurrently, and clears all locks' do
      20.times { |i| Delayed::Job.enqueue(ConcurrencyJob.new(i)) }
      Timeout.timeout(30) { dispatcher.run }

      expect(Delayed::Job.count).to eq 0
      expect(ConcurrencyJob.runs.keys.sort).to eq((0...20).to_a)
      expect(ConcurrencyJob.runs.values).to all(eq 1)
      expect(ConcurrencyJob.max_running).to be > 1
      expect(ConcurrencyJob.max_running).to be <= 4
    end

    it 'leaves no locks behind when stopped with jobs remaining' do
      Delayed::Worker.exit_on_complete = false
      8.times { |i| Delayed::Job.enqueue(ConcurrencyJob.new(i)) }
      allow(dispatcher.workers.first).to receive(:perform_job).and_wrap_original do |m, job|
        dispatcher.stop
        m.call(job)
      end
      Timeout.timeout(30) { dispatcher.run }

      expect(Delayed::Job.count).to be_between(1, 7)
      expect(Delayed::Job.where(:locked_by.ne => nil).count).to eq 0
    end
  end
end
