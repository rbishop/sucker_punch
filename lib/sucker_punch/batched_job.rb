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

        def batch(*after_args, after_klass, jobs)
          batch_id = Celluloid.uuid

          if jobs.is_a?(Hash)
            job_ids = jobs[:num].times.map { Celluloid.uuid }
            job_args = job_ids.map { |job_id| [batch_id, job_id].concat(jobs[:args] || []) }
          elsif jobs.is_a?(Array)
            job_args = jobs.map { |job| job.unshift(batch_id, Celluloid.uuid) }
            job_ids = job_args.map { |job| job[1] }
          end

          batch = {args: after_args.push([]), after: after_klass, ids: job_ids}
          Celluloid::Actor[self.class.batch_name].add_batch(batch_id, batch)
          job_args.each { |job| self.class.new.async.perform(*job) }
        end

        def notify(*args)
          publish(self.class.subscription_name, *args)
        end
      end
    end

    module ClassMethods
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
