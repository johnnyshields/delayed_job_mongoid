require 'spec_helper'

# Reads that must see the backend's own writes go to the primary, even when
# the app's client reads from secondaries by default.
describe 'Reads from the primary' do
  let(:job_class) { Delayed::Backend::Mongoid::Job }
  let(:read_preferences) { [] }

  # As an app reading from secondaries by default would configure it.
  around do |example|
    default = Mongoid.clients[:default] || Mongoid.clients['default']
    Mongoid.clients[:dj_secondary_reads] = default.merge(
      'options' => (default['options'] || default[:options] || {}).merge('read' => { 'mode' => :secondary_preferred })
    ).except(:options)
    Mongoid.override_client(:dj_secondary_reads)
    example.run
  ensure
    Mongoid.override_client(nil)
    Mongoid.clients.delete(:dj_secondary_reads)
  end

  before do
    job_class.delete_all
    prefs = read_preferences
    allow_any_instance_of(Mongo::Collection::View).to(receive(:each).and_wrap_original do |m, *args, &block|
      prefs << m.receiver.read_preference
      m.call(*args, &block)
    end)
  end

  after { Delayed::Worker.reset }

  it 'reads back the jobs a batch reservation just locked from the primary' do
    Delayed::Worker.batch_reserve = true
    2.times { job_class.create!(payload_object: SimpleJob.new) }
    workers = Array.new(2) { |i| Delayed::Worker.named("w#{i}") }
    expect(job_class.reserve_batch(workers).size).to eq 2
    expect(read_preferences).to eq [{ 'mode' => :primary }]
  end

  it 'lists the jobs locked by a worker from the primary' do
    job_class.create!(payload_object: SimpleJob.new, locked_by: 'w0', locked_at: job_class.db_time_now)
    expect(job_class.locked_jobs('w0').size).to eq 1
    expect(job_class.locked_jobs_of(%w[w0 w1]).size).to eq 1
    expect(read_preferences).to eq [{ 'mode' => :primary }] * 2
  end

  it "leaves other queries to the client's read preference" do
    job_class.where(locked_by: 'w0').to_a
    expect(read_preferences).to eq [nil]
  end
end
