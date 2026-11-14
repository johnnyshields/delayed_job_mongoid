# Delayed::Job Mongoid backend

[![Gem Version](https://badge.fury.io/rb/delayed_job_mongoid.svg)][gem]
[![CI](https://github.com/collectiveidea/delayed_job_mongoid/actions/workflows/ci.yml/badge.svg)][ci]
[![Coverage Status](https://coveralls.io/repos/github/collectiveidea/delayed_job_mongoid/badge.svg?branch=master)][coveralls]

[gem]: https://rubygems.org/gems/delayed_job_mongoid
[ci]: https://github.com/collectiveidea/delayed_job_mongoid/actions/workflows/ci.yml
[coveralls]: https://coveralls.io/github/collectiveidea/delayed_job_mongoid?branch=master

A [Mongoid](https://github.com/mongodb/mongoid) backend for
[delayed_job](https://github.com/collectiveidea/delayed_job).

See the [CHANGELOG](CHANGELOG.md) for what's new in each release.

## Requirements

delayed_job_mongoid requires Ruby 3.2 or later and Mongoid 7 or later. It is
tested against Mongoid 7, 8 and 9.

For Mongoid 5 and 6, use version 3.0 of this gem. For Mongoid 3 and 4, use
version 2.x.

## Installation

Add the gem to your `Gemfile`:

```ruby
gem 'delayed_job_mongoid'
```

Then create the indexes (don't forget to do this in production):

```
bin/rails runner 'Delayed::Backend::Mongoid::Job.create_indexes'
```

To generate `bin/delayed_job`:

```
bin/rails generate delayed_job
```

That's it. Use [delayed_job](https://github.com/collectiveidea/delayed_job) as normal.

## Advanced Usage

### Silencing Noisy Logging

By default, the Mongo Ruby Driver will print log lines every
5 seconds as Delayed Job polls your database. Use the following
plugin to silence logs which happen while polling (except `error`
level) and only log operations which happen during job execution.

```ruby
# in config/initializers/delayed_job.rb

require 'delayed/plugins/mongo_silencer'
Delayed::Worker.plugins << Delayed::Plugins::MongoSilencer
```

To ensure thread safety, this plugin requires your `Mongo::Logger.logger`
to be an instance of `ActiveSupport::Logger` class. It will ignore instances
of the Ruby standard library `Logger` class.

With multi-threaded workers (`--threads`), the plugin silences the job
reservation queries on the dispatcher thread as well, while each job thread
logs normally during job execution.

With a delayed_job that has `Delayed::Worker.silence_logging`, you can use it
instead of the plugin: it silences only the polling queries (reservations and
lock refreshes) on the polling thread, leaving job execution and the queries
that finish jobs logged. The same `ActiveSupport::Logger` requirement applies.

```ruby
Delayed::Worker.silence_logging = true # or --silence-logging / SILENCE_LOGGING=1
```

### Batched Reservation

With multi-threaded workers, filling N idle threads takes N `findAndModify`
round trips by default. With batched reservation, it takes 2 whatever N is: a
single bulk write of sorted `updateOne`s, each locking the next job for one
thread, then reading back the locked jobs. The server picks and locks each job
atomically, so none is lost to other processes. Requires MongoDB Server 8.0+
(sort on `updateOne`).

Turn it on with delayed_job's `batch_reserve` setting; it batches whenever two
or more threads are idle:

```ruby
# in config/initializers/delayed_job.rb
Delayed::Worker.batch_reserve = true
```

or `--batch-reserve` on the command line, or `BATCH_RESERVE=1` with
`rake jobs:work`.

Time to fill all threads of a worker process (median, local MongoDB 8.0, plus
simulated network latency per round trip; measured with an earlier 3-round-trip
version, so batching now saves one more latency):

| Threads | Local: off / on | +2ms: off / on | +5ms: off / on |
| --- | --- | --- | --- |
| 4 | 4.3 / 4.0 ms | 13.3 / 10.8 ms | 27.9 / 20.3 ms |
| 16 | 18.0 / 5.4 ms | 55.5 / 12.4 ms | 115.8 / 21.9 ms |
| 32 | 33.3 / 6.9 ms | 108.4 / 14.4 ms | 229.7 / 25.3 ms |

Recommended with 8 or more threads per process, or a remote database. It only
matters when many threads are idle at once (start-up, bursts of jobs); an idle
poll is one query either way.

### Customizing Job Class

Delayed Job allows specifying a custom backend class for jobs.
For example, you may wish to specify a different database
collection name. To do this:

```ruby
# in an initializer

class MyJob
  include Mongoid::Document

  store_in(collection: :my_jobs)

  include Delayed::Backend::Mongoid::Mixin
end

Delayed::Worker.backend = MyJob

# in your application

MyJob.enqueue(job_object)
```
