module WAZ
  module Queues
    # This class represents a Queue on Windows Azure Queues API. These are the methods implemented from Microsoft's API description 
    # available on MSDN at http://msdn.microsoft.com/en-us/library/dd179363.aspx
    #
    #	# list available queues
    #	WAZ::Queues::Queue.list
    #
    #	# create a queue (here you can also send hashed metadata)
    #	WAZ::Queues::Queue.create('test-queue')
    #
    #	# get a specific queue
    #	queue = WAZ::Queues::Queue.find('test-queue')
    #
    #	# get queue properties (including default headers)
    #	queue.metadata #=> hash containing beautified metadata (:x_ms_meta_name)
    #
    #	# set queue properties (should follow x-ms-meta to be persisted)
    #	# if you specify the optional parameter overwrite, existing metadata 
    #	# will be deleted else merged with new one.
    #	queue.put_properties!(:x_ms_meta_MyProperty => "my value")
    #
    #	# delete queue
    #	queue.destroy!
    #
    #	# clear queue contents
    #	queue.clear
    #
    #	# enqueue a message 
    #	queue.enqueue!("payload of the message")
    #
    #	# peek a message/s (do not alter visibility, it can't be deleted neither)
    #	# num_of_messages (1 to 32) to be peeked (default 1)
    #	message = queue.peek
    #
    #	# lock a message/s.
    #	# num_of_messages (1 to 32) to be peeked (default 1)
    #	# visiblity_timeout (default 60 sec. max 7200 [2hrs])
    #	message = queue.lock
    #
    class Queue
      class << self
        # Returns an array of the queues (WAZ::Queues::Queue) existing on the current 
        # Windows Azure Storage account.
        # 
        # include_metadata defines if the metadata is retrieved along with queue data.
        def list(include_metadata = false)
          options = include_metadata ? { :include => 'metadata' } : {}
          service_instance.list_queues(options).map do |queue|
            WAZ::Queues::Queue.new(queue)
          end
        end
        
        # Creates a queue on the current account. If provided the metadata hash will specify additional 
        # metadata to be stored on the queue. (Remember that metadata on the storage account must start with 
        # :x_ms_metadata_{yourCustomPropertyName}, if not it will not be persisted).
        def create(queue_name, metadata = {})
          raise WAZ::Storage::InvalidParameterValue, {:name => "name", :values => ["lower letters, numbers or - (hypen), and must not start or end with - (hyphen)"]} unless WAZ::Storage::ValidationRules.valid_name?(queue_name)
          service_instance.create_queue(queue_name, metadata)
          WAZ::Queues::Queue.new(:name => queue_name, :url => service_instance.generate_request_uri(queue_name))
        end
        
        # Finds a queue by it's name, in case that it isn't found on the current storage account it will 
        # return nil shilding the user from a ResourceNotFound exception.
        def find(queue_name)
          begin 
            metadata = service_instance.get_queue_metadata(queue_name)
            WAZ::Queues::Queue.new(:name => queue_name, :url => service_instance.generate_request_uri(queue_name), :metadata => metadata)
          rescue RestClient::ResourceNotFound
            return nil
          end
        end
        
        # Syntax's sugar for find(:queue_name) or create(:queue_name)
        def ensure(queue_name)
          return (self.find(queue_name) or self.create(queue_name))
        end
        
        # This method is internally used by this class. It's the way we keep a single instance of the 
        # service that wraps the calls the Windows Azure Queues API. It's initialized with the values
        # from the default_connection on WAZ::Storage::Base initialized thru establish_connection!
        def service_instance
          options = WAZ::Storage::Base.default_connection.merge(:type_of_service => "queue")
          (@service_instances ||= {})[options[:account_name]] ||= Service.new(options)
        end
      end
      
      attr_accessor :name, :url, :metadata

      def initialize(options = {})
        raise WAZ::Storage::InvalidOption, :name unless options.keys.include?(:name)
        raise WAZ::Storage::InvalidOption, :url unless options.keys.include?(:url)
        self.name = options[:name]
        self.url = options[:url]
        self.metadata = options[:metadata]
      end
      
      # Deletes the queue from the current storage account.
      def destroy!
        self.class.service_instance.delete_queue(self.name)
      end
      
      # Retrieves the metadata headers associated with the quere.
      def metadata
        metadata ||= self.class.service_instance.get_queue_metadata(self.name)
      end
      
      # Sets the metadata given on the new_metadata, when overwrite passed different 
      # than true it overrides the metadata for the queue  (removing all existing metadata)
      def put_properties!(new_metadata = {}, overwrite = false)
        new_metadata.merge!(metadata.reject { |k, v| !k.to_s.start_with? "x_ms_meta"} ) unless overwrite
        self.class.service_instance.set_queue_metadata(new_metadata)
      end
      
      # Enqueues a message on current queue. message is just a string that should be 
      # UTF-8 serializable and ttl specifies the time-to-live of the message in the queue 
      # (in seconds).
      def enqueue!(message, ttl = 604800)
        self.class.service_instance.enqueue(self.name, message, ttl)
      end
      
      # Returns the approximated queue size.
      def size
        metadata[:x_ms_approximate_messages_count].to_i
      end
      
      # Since Windows Azure Queues implement a Peek-Lock pattern 
      # the method lock will lock a message preventing other users from 
      # picking/locking the current message from the queue. 
      #
      # The API supports multiple message processing by specifiying num_of_messages (up to 32)
      # 
      # The visibility_timeout parameter (optional) specifies for how long the message will be 
      # hidden from other users.
      def lock(num_of_messages = 1, visibility_timeout = nil)
        options = {}
        options[:num_of_messages] = num_of_messages
        options[:visiblity_timeout] = visibility_timeout unless visibility_timeout.nil?
        messages = self.class.service_instance.get_messages(self.name, options).map do |raw_message|
                    WAZ::Queues::Message.new(raw_message.merge(:queue_name => self.name))
                  end
        return messages.first() if num_of_messages == 1
        return messages
      end
      
      # Returns top N (default 1, up to 32) message from the queue without performing 
      # any modification on the message. Since the message it's retrieved read-only
      # users cannot delete the peeked message.
      def peek(num_of_messages = 1)
        options = {}
        options[:num_of_messages] = num_of_messages
        messages = self.class.service_instance.peek(self.name, options).map do |raw_message|
                    WAZ::Queues::Message.new(raw_message.merge(:queue_name => self.name))
                  end
        return messages.first() if num_of_messages == 1
        return messages
      end
      
      # Marks every message on the queue for deletion (to be later garbage collected).
      def clear
        self.class.service_instance.clear_queue(self.name)
      end
    end
  end
end