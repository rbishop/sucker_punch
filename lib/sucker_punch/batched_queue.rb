module SuckerPunch
  class BatchedQueue
    include ::Celluloid
    include ::Celluloid::Notifications

    def initialize(klass)
      @klass = klass
      @batches = {}
      @mutex = Mutex.new
      setup_fanout_notifier
      subscribe(subscription_name, :handle_job_completed)
    end

    def add_batch(batch_id, job_ids, args = [])
      @mutex.synchronize do
        @batches[batch_id] = {args: args, ids: job_ids}
      end
    end

    def handle_job_completed(topic, batch_id, job_id)
      @mutex.synchronize do
        begin
          batch = @batches.fetch(batch_id)
          batch[:ids].delete(job_id)
          schedule_after_job(batch[:args]) if batch[:ids].empty?
        rescue KeyError
          # If the batch doesn't exist, just ignore it.
        end
      end
    end

    private

    def schedule_after_job(args)
      after_job = @klass.after_batch.new.async

      if args.any?
        after_job.perform(*args)
      else
        after_job.perform
      end
    end

    def setup_fanout_notifier
      @mutex.synchronize do
        unless Celluloid::Actor[:notifications_fanout]
          Celluloid::Actor[:notifications_fanout] = Celluloid::Notifications::Fanout.new
        end
      end
    end

    def batch_name
      "#{@klass.to_s.underscore}_batch".to_sym
    end

    def subscription_name
      "#{@klass.to_s.underscore}_job_complete"
    end
  end
end
