require 'celluloid/autostart'

module SuckerPunch
  class BatchedQueue
    include ::Celluloid
    include ::Celluloid::Notifications

    def initialize(klass)
      puts "After commit"
      @klass = klass
      @batches = {}
      @mutex = Mutex.new
    end

    def add_batch(batch_id, job_ids, args = [])
      @mutex.synchronize do
        @batches[batch_id] = {args: args, ids: job_ids}
        subscribe('job_in_batch_completed', :handle_job_completed)
      end
    end

    def handle_job_completed(topic, batch_id, job_id)
      @mutex.synchronize do
        begin
          batch = @batches.fetch(batch_id)
          batch[:ids].delete(job_id)

          if batch[:ids].empty?
            @klass.after_batch.new.async.perform(batch[:args])
          end
        rescue KeyError
          # Ignore if the batch doesnt exist
        end
      end
    end

    def name
      @klass.to_s.underscore.to_sym
    end
    
    def register
      Celluloid::Actor[name] = self
    end
  end
end
