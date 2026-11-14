require 'spec_helper'

# Sleeps, so that its worker refreshes its lock meanwhile.
SilencePollingSleepJob = Struct.new(:seconds) do
  def perform
    sleep seconds
  end
end

describe 'Delayed::Worker.silence_logging with Mongoid' do
  let(:mongo_io) { StringIO.new }
  let(:mongo_logger) { ActiveSupport::Logger.new(mongo_io).tap { |l| l.level = Logger::DEBUG } }
  # Ruby 3.4 changed Hash#inspect from {"a"=>1} to {"a" => 1}
  let(:find_and_modify_jobs) { /"findAndModify" ?=> ?"delayed_backend_mongoid_jobs"/ }
  let(:find_stories) { /"find" ?=> ?"stories"/ }
  # Delayed::Job.touch_locks (clearing locks on exit sets locked_by too).
  let(:lock_refresh) { /"locked_by" ?=> ?\{"\$in"[^}]*\}\}, "u" ?=> ?\{"\$set" ?=> ?\{"locked_at"/ }

  before do
    Mongo::Logger.logger = mongo_logger
    Delayed::Worker.sleep_delay = 0.05
  end

  after do
    Delayed::Worker.reset
    Delayed::Worker.exit_on_complete = false
    Mongo::Logger.logger = $mongo_logger
  end

  def run_to_completion
    dispatcher = Delayed::Dispatcher.new(threads: 2, quiet: true)
    Delayed::Worker.exit_on_complete = true
    Timeout.timeout(10) { dispatcher.run }
  end

  it 'logs the reservation queries when off' do
    run_to_completion
    expect(mongo_io.string).to match(find_and_modify_jobs)
  end

  it 'logs lock refreshes when off' do
    Delayed::Worker.lock_heartbeat_interval = 0.05
    Delayed::Worker.lock_timeout = 1
    Delayed::Job.create(payload_object: SilencePollingSleepJob.new(0.3))
    run_to_completion
    expect(mongo_io.string).to match(lock_refresh)
  end

  context 'when on' do
    before { Delayed::Worker.silence_logging = true }

    it 'does not log the reservation queries' do
      run_to_completion
      expect(mongo_io.string).not_to match(find_and_modify_jobs)
    end

    it "logs the jobs' own queries" do
      Delayed::Job.create(payload_object: StoryWrapperJob.new)
      run_to_completion
      expect(mongo_io.string).to match(find_stories)
      expect(mongo_io.string).not_to match(find_and_modify_jobs)
    end

    it 'does not log lock refreshes' do
      Delayed::Worker.lock_heartbeat_interval = 0.05
      Delayed::Worker.lock_timeout = 1
      touched = 0
      allow(Delayed::Job).to(receive(:touch_locks).and_wrap_original do |m, names|
        touched += 1
        m.call(names)
      end)
      Delayed::Job.create(payload_object: SilencePollingSleepJob.new(0.3))
      run_to_completion
      expect(touched).to be >= 2
      expect(mongo_io.string).not_to match(lock_refresh)
    end
  end

  describe 'Delayed::Job.silence_logging' do
    it 'silences the Mongo logger below WARN in the block, on this thread only' do
      other = nil
      result = Delayed::Job.silence_logging do
        mongo_logger.info('quiet')
        mongo_logger.warn('loud')
        other = Thread.new { mongo_logger.info('other thread') }.join
        :result
      end
      expect(result).to eq :result
      expect(mongo_io.string).not_to include 'quiet'
      expect(mongo_io.string).to include 'loud'
      expect(mongo_io.string).to include 'other thread'
      expect(other).not_to be_nil
    end

    it 'just yields with a logger that cannot silence' do
      plain_io = StringIO.new
      Mongo::Logger.logger = Logger.new(plain_io)
      expect(Delayed::Job.silence_logging do
        Mongo::Logger.logger.info('logged')
        :ran
      end).to eq :ran
      expect(plain_io.string).to include 'logged'
    end
  end
end
