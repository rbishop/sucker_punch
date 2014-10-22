module SuckerPunch
  class BatchedQueue
    include ::Celluloid
    include ::Celluloid::Notifications

    def initialize(klass)
      @klass = klass
      @batches = {}
      @mutex = Mutex.new
      setup_fanout_notifier
      subscribe(klass.subscription_name, :handle_job_completed)
    end

    def add_batch(batch_id, batch)
      @mutex.synchronize { @batches[batch_id] = batch }
    end

    def handle_job_completed(topic, batch_id, job_id, *args_from_jobs)
      @mutex.synchronize do
        begin
          batch = @batches.fetch(batch_id)
          batch[:args].last.concat(args_from_jobs) if args_from_jobs.any?
          batch[:ids].delete(job_id)
          schedule_after_job(batch[:after], batch[:args]) if batch[:ids].empty?
        rescue KeyError
          # If the batch doesn't exist, just ignore it.
        end
      end
    end

    private

    def schedule_after_job(klass, args)
      if args.any?
        klass.new.async.perform(*args)
      else
        klass.new.async.perform
      end
    end

    def setup_fanout_notifier
      @mutex.synchronize do
        unless Celluloid::Actor[:notifications_fanout]
          Celluloid::Actor[:notifications_fanout] = Celluloid::Notifications::Fanout.new
        end
      end
    end
  end
end
