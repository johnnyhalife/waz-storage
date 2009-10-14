module WAZ
  module Queues
    class Queue
      class << self
        def list
          service_instance.list_queues.map do |queue|
            WAZ::Queues::Queue.new(queue)
          end
        end
        
        def create(queue_name, metadata = {})
          service_instance.create_queue(queue_name, metadata)
          WAZ::Queues::Queue.new(:name => queue_name, :url => service_instance.generate_request_uri(nil, queue_name))
        end
        
        def find(queue_name)
          begin 
            service_instance.get_queue_metadata(queue_name)
            WAZ::Queues::Queue.new(:name => queue_name, :url => service_instance.generate_request_uri(nil, queue_name))
          rescue RestClient::ResourceNotFound
            return nil
          end
        end
        
        private
          def service_instance
            options = WAZ::Storage::Base.default_connection
            @service_instance ||= Service.new(options[:account_name], options[:access_key], "queue")
          end
      end
      
      attr_accessor :name, :url

      def initialize(options = {})
        raise WAZ::Storage::InvalidOption, :name unless options.keys.include?(:name)
        raise WAZ::Storage::InvalidOption, :url unless options.keys.include?(:url)
        self.name = options[:name]
        self.url = options[:url]
      end
      
      def destroy!
        service_instance.delete_queue(self.name)
      end
      
      def metadata
        service_instance.get_queue_metadata(self.name)
      end
      
      # when overwrite passed different than true it overrides 
      # the metadata for the queue 
      def put_properties!(new_metadata = {}, overwrite = false)
        new_metadata.merge!(metadata.reject { |k, v| !k.to_s.start_with? "x_ms_meta"} ) unless overwrite
        service_instance.set_queue_metadata(new_metadata)
      end
      
      def enqueue!(message, ttl = 604800)
        service_instance.enqueue(self.name, message, ttl)
      end
      
      def size
        metadata[:x_ms_approximate_messages_count].to_i
      end
      
      def lock(num_of_messages = 1, visibility_timeout = nil)
        options = {}
        options[:num_of_messages] = num_of_messages
        options[:visiblity_timeout] = visibility_timeout unless visibility_timeout.nil?
        messages = service_instance.get_messages(self.name, options).map do |raw_message|
                    WAZ::Queues::Message.new(raw_message.merge(:queue_name => self.name))
                  end
        return messages.first() if num_of_messages == 1
        return messages
      end
      
      def peek(num_of_messages = 1)
        options = {}
        options[:num_of_messages] = num_of_messages
        messages = service_instance.peek(self.name, options).map do |raw_message|
                    WAZ::Queues::Message.new(raw_message.merge(:queue_name => self.name))
                  end
        return messages.first() if num_of_messages == 1
        return messages
      end
      
      def clear
        service_instance.clear_queue(self.name)
      end
      
      private
        def service_instance
          options = WAZ::Storage::Base.default_connection
          @service_instance ||= Service.new(options[:account_name], options[:access_key], "queue")
        end
    end
  end
end