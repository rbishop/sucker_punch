require 'celluloid/autostart'

module SuckerPunch
  module BatchedJob
    def self.included(base)
      base.send(:include, ::Celluloid)
      base.send(:include, ::Celluloid::Notifications)
      base.extend(ClassMethods)

      base.class_eval do
        def self.new
          define_batched_actor(self)
          define_celluloid_pool(self, @workers)
        end

        def self.run_after_batch(klass)
          @after_batch = klass
        end

        def self.after_batch
          @after_batch
        end
      end
    end

    module ClassMethods
      def workers(num)
        @workers = num
      end

      def define_celluloid_pool(klass, num_workers)
        SuckerPunch::Queue.new(klass).register(num_workers)
      end

      def define_batched_actor(klass)
        unless Celluloid::Actor.registered.include? "#{klass.to_s.underscore}_batch".to_sym
          actor = SuckerPunch::BatchedQueue.new(klass)
          actor.register(actor)
        end
      end
    end
  end
end
