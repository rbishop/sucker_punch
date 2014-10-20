module SuckerPunch
  module BatchedJob
    def self.included(base)
      puts "Included!"
      base.send(:include, ::Celluloid)
      base.send(:include, ::Celluloid::Notifications)
      base.extend(ClassMethods)

      base.class_eval do
        def self.new
          define_celluloid_pool(self, @workers)
          SuckerPunch::BatchedQueue.new(self)
        end
      end
    end

    module ClassMethods
      def workers(num)
        @workers = num
      end

      def self.after_batch
        @after_batch
      end

      def define_celluloid_pool(klass, num_workers)
        SuckerPunch::Queue.new(klass).register(num_workers)
      end
    end
  end
end
