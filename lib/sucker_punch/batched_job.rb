module SuckerPunch
  module BatchedJob
    def self.included(base)
      base.send(:include, SuckerPunch::Job)
      base.send(:include, ::Celluloid::Notifications)
      base.extend(ClassMethods)

      base.class_eval do
        def self.new
          define_batched_actor(self)
          define_celluloid_pool(self, @workers)
        end

        def batch(batch_args, *after_job_args)
          batch_id = Celluloid.uuid

          batch_args = batch_args.map { |job| job.unshift(batch_id, Celluloid.uuid) }
          job_ids = batch_args.map { |job| job[1] }

          Celluloid::Actor[self.class.batch_name].add_batch(batch_id, job_ids, after_job_args)
          batch_args.each { |job| self.class.new.async.perform(*job) }
        end

        def notify(*args)
          publish(self.class.subscription_name, *args)
        end
      end
    end

    module ClassMethods
      def run_after_batch(klass)
        @after_batch = klass
      end

      def after_batch
        @after_batch
      end

      def define_batched_actor(klass)
        unless Celluloid::Actor.registered.include?(batch_name)
          actor = SuckerPunch::BatchedQueue.new(klass)
          Celluloid::Actor[batch_name] = actor
        end
      end
      
      def batch_name
        "#{name.to_s.underscore}_batch".to_sym
      end

      def subscription_name
        "#{name.to_s.underscore}_job_complete"
      end
    end
  end
end
