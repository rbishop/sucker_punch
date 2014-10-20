require 'celluloid/autostart'

module SuckerPunch
  class BatchedQueue
    include ::Celluloid
    include ::Celluloid::Notifications

    def initialize(klass)
      @klass = klass
      @batches = {}
      @mutex = Mutex.new
      subscribe('job_in_batch_completed', :handle_job_completed)
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

          if batch[:ids].empty?
            @klass.after_batch.new.async.perform(batch[:args])
          end
        rescue KeyError
          # Ignore if the batch doesnt exist
        end
      end
    end

    def name
      "#{@klass.to_s.underscore}_batch".to_sym
    end
    
    def register(actor)
      @mutex.synchronize do
        unless registered?
          Celluloid::Actor[name] = actor
        end
      end

      Celluloid::Actor[name]
    end

    def registered?
      Celluloid::Actor.registered.include?(name.to_sym)
    end
  end
end
