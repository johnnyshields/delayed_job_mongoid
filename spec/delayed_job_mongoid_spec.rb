require 'spec_helper'

describe Delayed::Backend::Mongoid::Job do
  it_behaves_like 'a delayed_job backend'
  it_behaves_like 'a delayed_job backend supporting lock heartbeats'
  it_behaves_like 'a delayed_job backend running a dispatcher'

  context 'with batch_reserve (one bulk write per poll)' do
    around do |example|
      Delayed::Backend::Mongoid.batch_reserve = true
      example.run
    ensure
      Delayed::Backend::Mongoid.reset_configuration!
    end

    it_behaves_like 'a delayed_job backend running a dispatcher'
  end

  describe '#save_if_locked_by' do
    it 'writes only the changed fields' do
      job = Delayed::Job.create!(payload_object: SimpleJob.new, locked_by: 'w1', locked_at: Delayed::Job.db_time_now)
      Delayed::Job.where(_id: job.id).update_all('$set' => { queue: 'changed elsewhere' })
      job.attempts = 2
      expect(job.save_if_locked_by('w1')).to be true
      saved = Delayed::Job.find(job.id)
      expect([saved.attempts, saved.queue]).to eq [2, 'changed elsewhere']
      expect(job.changed?).to be false
    end

    it 'updates updated_at' do
      job = Delayed::Job.create!(payload_object: SimpleJob.new, locked_by: 'w1', locked_at: Delayed::Job.db_time_now)
      before = Delayed::Job.find(job.id).updated_at
      sleep 0.01
      job.attempts = 1
      job.save_if_locked_by('w1')
      expect(Delayed::Job.find(job.id).updated_at).to be > before
    end

    it 'runs the save callbacks, which set the default run_at' do
      job = Delayed::Job.create!(payload_object: SimpleJob.new, locked_by: 'w1', locked_at: Delayed::Job.db_time_now)
      job.run_at = nil
      job.save_if_locked_by('w1')
      expect(Delayed::Job.find(job.id).run_at).not_to be_nil
    end
  end

  context 'when job contains two instance variables that reference the same Mongoid document' do
    it 'can serialize and deserialize the job' do
      job = Delayed::Job.create(payload_object: StoryWrapperJob.new)
      expect(job.reload.payload_object).to be_a StoryWrapperJob
    end
  end

  context 'when a serialized Mongoid document no longer exists' do
    let(:yaml) do
      story = Story.create!(text: 'My lost story')
      story.to_yaml.tap { story.destroy }
    end

    around do |example|
      raise_not_found_error = Mongoid.raise_not_found_error
      Mongoid.raise_not_found_error = false
      example.run
    ensure
      Mongoid.raise_not_found_error = raise_not_found_error
    end

    it 'deserializes it as nil if Mongoid does not raise' do
      expect(YAML.load_dj(yaml)).to be_nil
    end
  end

  describe '.locked_jobs' do
    it 'returns only the jobs locked by the given worker' do
      now = Delayed::Job.db_time_now
      mine = Delayed::Job.create!(payload_object: SimpleJob.new, locked_by: 'dj.1 pid:7 thread:0', locked_at: now)
      Delayed::Job.create!(payload_object: SimpleJob.new, locked_by: 'dj.1 pid:7 thread:1', locked_at: now)
      Delayed::Job.create!(payload_object: SimpleJob.new)
      expect(Delayed::Job.locked_jobs('dj.1 pid:7 thread:0').map(&:id)).to eq [mine.id]
    end
  end

  describe 'recovering the jobs of a dead worker process' do
    let(:status) { instance_double(Process::Status, signaled?: true, termsig: 9) }
    let(:error) { Delayed::ProcessExitError.new('dj.1', 7, status) }

    it 'counts each as a failed attempt and unlocks it' do
      job = Delayed::Job.create!(payload_object: SimpleJob.new, locked_by: 'dj.1 pid:7 thread:0',
                                 locked_at: Delayed::Job.db_time_now)
      expect(Delayed::Worker.recover_jobs_of(['dj.1 pid:7 thread:0', 'dj.1 pid:7 thread:1'], error)).to eq 1

      job.reload
      expect(job.attempts).to eq 1
      expect(job.locked_by).to be_nil
      expect(job.locked_at).to be_nil
      expect(job.last_error).to start_with 'Child process dj.1 (pid 7) was killed by signal 9'
      expect(job.run_at).to be > Delayed::Job.db_time_now
    end
  end

  describe '.before_fork' do
    it 'disconnects Mongoid' do
      expect(Mongoid).to receive(:disconnect_clients)
      Delayed::Job.before_fork
    end
  end

  describe '.after_fork' do
    # ensure Mongoid is connected
    before { Delayed::Job.first }

    it 'reconnects Mongoid' do
      expect_any_instance_of(Mongo::Client).to receive(:close)
      expect_any_instance_of(Mongo::Client).to receive(:reconnect)
      Delayed::Job.after_fork
    end
  end

  context 'custom job class' do
    let(:custom_job_class) do
      Class.new do
        include Mongoid::Document

        store_in(collection: :my_jobs)
        include Delayed::Backend::Mongoid::Mixin
      end
    end

    it 'can use a custom job class' do
      expect(custom_job_class.collection.name).to eq 'my_jobs'
      expect(Delayed::Job.all.count).to eq 0
      expect(custom_job_class.all.count).to eq 0
      custom_job_class.enqueue(StoryWrapperJob.new)
      expect(Delayed::Job.all.count).to eq 0
      expect(custom_job_class.count).to eq 1
      expect(custom_job_class.all.first.reload.payload_object).to be_a StoryWrapperJob
    end
  end
end
