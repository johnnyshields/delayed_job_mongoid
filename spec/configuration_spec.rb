require 'spec_helper'

describe Delayed::Backend::Mongoid::Configuration do
  after { Delayed::Backend::Mongoid.reset_configuration! }

  it 'defaults to batched reservation off, with a threshold of 4' do
    config = described_class.new
    expect([config.batch_reserve, config.batch_reserve_threshold]).to eq [false, 4]
  end

  it 'stores batch_reserve as a boolean' do
    config = described_class.new
    config.batch_reserve = 'yes'
    expect(config.batch_reserve).to be true
    config.batch_reserve = nil
    expect(config.batch_reserve).to be false
  end

  it 'accepts a threshold of 1 or more' do
    config = described_class.new
    config.batch_reserve_threshold = 1
    expect(config.batch_reserve_threshold).to eq 1
  end

  [0, -1, 2.5, '4', nil].each do |value|
    it "rejects a threshold of #{value.inspect}" do
      config = described_class.new
      expect { config.batch_reserve_threshold = value }.to raise_error(
        ArgumentError, "batch_reserve_threshold must be an Integer of 1 or more, got #{value.inspect}"
      )
      expect(config.batch_reserve_threshold).to eq 4
    end
  end

  describe 'Delayed::Backend::Mongoid' do
    subject(:backend) { Delayed::Backend::Mongoid }

    it 'yields its configuration to configure, and returns it' do
      result = backend.configure do |config|
        config.batch_reserve = true
        config.batch_reserve_threshold = 8
      end
      expect(result).to equal backend.configuration
      expect([backend.batch_reserve, backend.batch_reserve_threshold]).to eq [true, 8]
    end

    it 'sets the settings directly too' do
      backend.batch_reserve = true
      backend.batch_reserve_threshold = 2
      expect([backend.configuration.batch_reserve, backend.configuration.batch_reserve_threshold]).to eq [true, 2]
    end

    it 'restores the defaults with reset_configuration!' do
      backend.configure { |config| config.batch_reserve = true }
      backend.reset_configuration!
      expect([backend.batch_reserve, backend.batch_reserve_threshold]).to eq [false, 4]
    end

    it 'is not set on the job class' do
      expect(Delayed::Backend::Mongoid::Job).not_to respond_to(:batch_reserve)
      expect(Delayed::Backend::Mongoid::Job).not_to respond_to(:batch_reserve=)
      expect(Delayed::Backend::Mongoid::Job).not_to respond_to(:batch_reserve_threshold=)
    end

    it 'is what the job class reserves by' do
      backend.batch_reserve = true
      expect(Delayed::Backend::Mongoid::Job.partial_batch_may_leave_jobs?).to be true
      backend.batch_reserve = false
      expect(Delayed::Backend::Mongoid::Job.partial_batch_may_leave_jobs?).to be false
    end
  end
end
