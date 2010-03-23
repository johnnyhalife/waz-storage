module WAZ
  module Queues
    # This is internally used by the waz-queues part of the gem and it exposes the Windows Azure Queue API REST methods 
    # implementation. You can use this class to perform an specific operation that aren't provided by the current API.
    class Service
      include WAZ::Storage::SharedKeyCoreService
      
      # Lists the queues on the given storage account.
      #
      # When the options :include => 'metadata' is passed it returns
      # the corresponding metadata for each queue on the listing.
      def list_queues(options ={})
        content = execute(:get, nil, { :comp => 'list' }.merge!(options), { :x_ms_version => "2009-09-19" })
        doc = REXML::Document.new(content)
        queues = []
        
        REXML::XPath.each(doc, '//Queue/') do |item|
          metadata = {}
          
          item.elements['Metadata'].elements.each do |element|
            metadata.merge!(element.name.gsub(/-/, '_').downcase.to_sym => element.text)
          end unless item.elements['Metadata'].nil?
          
          queues << { :name => REXML::XPath.first(item, "Name").text,
                      :url => REXML::XPath.first(item, "Url").text,
                      :metadata => metadata}
        end
        return queues
      end
      
      # Creates a queue on the current storage account. Throws WAZ::Queues::QueueAlreadyExists when 
      # existing metadata and given metadata differ.
      def create_queue(queue_name, metadata = {})
        execute(:put, queue_name, nil, metadata.merge!(:x_ms_version => '2009-09-19'))
      end
      
      # Deletes the given queue from the current storage account.
      def delete_queue(queue_name)
        execute(:delete, queue_name, {}, {:x_ms_version => '2009-09-19'})
      end
      
      # Gets the given queue metadata.
      def get_queue_metadata(queue_name)
        execute(:head, queue_name, { :comp => 'metadata'}, :x_ms_version => '2009-09-19').headers
      end
      
      # Sets the given queue metadata.
      def set_queue_metadata(queue_name, metadata = {})
        execute(:put, queue_name, { :comp => 'metadata' }, metadata.merge!(:x_ms_version => '2009-09-19'))
      end
      
      # Enqueues a message on the current queue.
      #
      # ttl Specifies the time-to-live interval for the message, in seconds. The maximum time-to-live allowed is 7 days. If this parameter
      # is omitted, the default time-to-live is 7 days.
      def enqueue(queue_name, message_payload, ttl = 604800)
        payload = "<?xml version=\"1.0\" encoding=\"utf-8\"?><QueueMessage><MessageText>#{message_payload}</MessageText></QueueMessage>"
        execute(:post, "#{queue_name}/messages", { :messagettl => ttl }, { 'Content-Type' => 'application/xml', :x_ms_version => "2009-09-19"}, payload)
      end
      
      # Locks N messages (1 default) from the given queue.
      #
      # :num_of_messages option specifies the max number of messages to get (maximum 32)
      #
      # :visibility_timeout option specifies the timeout of the message locking in seconds (max two hours)
      def get_messages(queue_name, options = {})
        raise WAZ::Queues::OptionOutOfRange, {:name => :num_of_messages, :min => 1, :max => 32} if (options.keys.include?(:num_of_messages) && (options[:num_of_messages].to_i < 1 || options[:num_of_messages].to_i > 32))
        raise WAZ::Queues::OptionOutOfRange, {:name => :visibility_timeout, :min => 1, :max => 7200} if (options.keys.include?(:visibility_timeout) && (options[:visibility_timeout].to_i < 1 || options[:visibility_timeout].to_i > 7200))
        content = execute(:get, "#{queue_name}/messages", options, {:x_ms_version => "2009-09-19"})
        doc = REXML::Document.new(content)
        messages = []
        REXML::XPath.each(doc, '//QueueMessage/') do |item|
          message = { :message_id => REXML::XPath.first(item, "MessageId").text,
                      :message_text => REXML::XPath.first(item, "MessageText").text,
                      :dequeue_count => REXML::XPath.first(item, "DequeueCount").nil? ? nil : REXML::XPath.first(item, "DequeueCount").text.to_i,
                      :expiration_time => Time.httpdate(REXML::XPath.first(item, "ExpirationTime").text),
                      :insertion_time => Time.httpdate(REXML::XPath.first(item, "InsertionTime").text) }

          # This are only valid when peek-locking messages
          message[:pop_receipt] = REXML::XPath.first(item, "PopReceipt").text unless REXML::XPath.first(item, "PopReceipt").nil?
          message[:time_next_visible] = Time.httpdate(REXML::XPath.first(item, "TimeNextVisible").text) unless REXML::XPath.first(item, "TimeNextVisible").nil?
          messages << message
        end
        return messages
      end
      
      # Peeks N messages (default 1) from the given queue.
      # 
      # Implementation is the same of get_messages but differs on an additional parameter called :peek_only.
      def peek(queue_name, options = {})
        return get_messages(queue_name, {:peek_only => true}.merge(options))
      end
      
      # Deletes the given message from the queue, correlating the operation with the pop_receipt
      # in order to avoid eventually inconsistent scenarios.
      def delete_message(queue_name, message_id, pop_receipt)
        execute :delete, "#{queue_name}/messages/#{message_id}", { :pop_receipt => pop_receipt }
      end
      
      # Marks every message on the given queue for deletion.
      def clear_queue(queue_name)
        execute :delete, "#{queue_name}/messages", {}, :x_ms_version => '2009-09-19'
      end
    end
  end
end