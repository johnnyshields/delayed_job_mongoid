require 'spec_helper'

describe Delayed::Backend::Mongoid::Configuration do
  after { Delayed::Backend::Mongoid.reset_configuration! }

  it 'defaults to batched reservation off' do
    expect(described_class.new.batch_reserve).to be false
  end

  it 'stores batch_reserve as a boolean' do
    config = described_class.new
    config.batch_reserve = 'yes'
    expect(config.batch_reserve).to be true
    config.batch_reserve = nil
    expect(config.batch_reserve).to be false
  end

  it 'has no threshold setting' do
    expect(described_class.new).not_to respond_to(:batch_reserve_threshold)
    expect(Delayed::Backend::Mongoid).not_to respond_to(:batch_reserve_threshold=)
  end

  describe 'Delayed::Backend::Mongoid' do
    subject(:backend) { Delayed::Backend::Mongoid }

    it 'yields its configuration to configure, and returns it' do
      result = backend.configure { |config| config.batch_reserve = true }
      expect(result).to equal backend.configuration
      expect(backend.batch_reserve).to be true
    end

    it 'sets the setting directly too' do
      backend.batch_reserve = true
      expect(backend.configuration.batch_reserve).to be true
    end

    it 'exists from the start, so threads reading it at once share it' do
      expect(backend).not_to respond_to(:configuration=)
      expect(backend.method(:configuration).source_location.first).to end_with 'mongoid_configuration.rb'
      expect(backend.configuration).to be_a described_class
      configs = Array.new(8) { Thread.new { backend.configuration } }.map(&:value)
      expect(configs.uniq.size).to eq 1
    end

    it 'restores the default with reset_configuration!' do
      backend.configure { |config| config.batch_reserve = true }
      backend.reset_configuration!
      expect(backend.batch_reserve).to be false
    end

    it 'is not set on the job class' do
      expect(Delayed::Backend::Mongoid::Job).not_to respond_to(:batch_reserve)
      expect(Delayed::Backend::Mongoid::Job).not_to respond_to(:batch_reserve=)
    end

    it 'is what the job class reserves by' do
      allow(Delayed::Backend::Mongoid::Job).to receive(:sorted_updates?).and_return(false)
      backend.batch_reserve = true
      expect(Delayed::Backend::Mongoid::Job.partial_batch_may_leave_jobs?).to be true
      backend.batch_reserve = false
      expect(Delayed::Backend::Mongoid::Job.partial_batch_may_leave_jobs?).to be false
    end
  end
end
