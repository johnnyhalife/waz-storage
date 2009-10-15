module WAZ
  module Queues
    class Service
      include WAZ::Storage::SharedKeyCoreService
      
      def list_queues(options ={})
        url = generate_request_uri(nil, :comp => 'list')
        request = generate_request("GET", url)
        doc = REXML::Document.new(request.execute())
        queues = []
         REXML::XPath.each(doc, '//Queue/') do |item|
            queues << { :name => REXML::XPath.first(item, "QueueName").text,
                        :url => REXML::XPath.first(item, "Url").text }
          end
        return queues
      end
      
      def create_queue(queue_name, metadata = {})
        begin
          url = generate_request_uri(queue_name)
          request = generate_request("PUT", url, metadata)
          request.execute()
        rescue RestClient::RequestFailed
          raise WAZ::Queues::QueueAlreadyExists, queue_name if $!.http_code == 409
        end
      end
      
      def delete_queue(queue_name)
        url = generate_request_uri(queue_name)
        request = generate_request("DELETE", url)
        request.execute()
      end
      
      def get_queue_metadata(queue_name)
        url = generate_request_uri(queue_name, :comp => 'metadata')
        request = generate_request("HEAD", url)
        request.execute().headers
      end
      
      def set_queue_metadata(queue_name, metadata = {})
        url = generate_request_uri(queue_name, :comp => 'metadata')
        request = generate_request("PUT", url, metadata)
        request.execute()
      end
      
      # ttl Specifies the time-to-live interval for the message, in seconds. 
      # The maximum time-to-live allowed is 7 days. If this parameter is omitted, the default time-to-live is 7 days.
      def enqueue(queue_name, message_payload, ttl = 604800)
        url = generate_request_uri("#{queue_name}/messages", "messagettl" => ttl)
        payload = "<?xml version=\"1.0\" encoding=\"utf-8\"?><QueueMessage><MessageText>#{message_payload}</MessageText></QueueMessage>"
        request = generate_request("POST", url, { "Content-Type" => "application/xml" }, payload)
        request.execute()
      end
      
      # :num_of_messages option specifies the max number of messages to get (maximum 32)
      # :visibility_timeout option specifies the timeout of the message locking in seconds (max two hours)
      def get_messages(queue_name, options = {})
        raise WAZ::Queues::OptionOutOfRange, {:name => :num_of_messages, :min => 1, :max => 32} if (options.keys.include?(:num_of_messages) && (options[:num_of_messages].to_i < 1 || options[:num_of_messages].to_i > 32))
        raise WAZ::Queues::OptionOutOfRange, {:name => :visibility_timeout, :min => 1, :max => 7200} if (options.keys.include?(:visibility_timeout) && (options[:visibility_timeout].to_i < 1 || options[:visibility_timeout].to_i > 7200))
        url = generate_request_uri("#{queue_name}/messages", options)
        request = generate_request("GET", url)
        doc = REXML::Document.new(request.execute())
        messages = []
        REXML::XPath.each(doc, '//QueueMessage/') do |item|
          message = { :message_id => REXML::XPath.first(item, "MessageId").text,
                      :message_text => REXML::XPath.first(item, "MessageText").text,
                      :expiration_time => Time.httpdate(REXML::XPath.first(item, "ExpirationTime").text),
                      :insertion_time => Time.httpdate(REXML::XPath.first(item, "InsertionTime").text) }

          # This are only valid when peek-locking messages
          message[:pop_receipt] = REXML::XPath.first(item, "PopReceipt").text unless REXML::XPath.first(item, "PopReceipt").nil?
          message[:time_next_visible] = Time.httpdate(REXML::XPath.first(item, "TimeNextVisible").text) unless REXML::XPath.first(item, "TimeNextVisible").nil?
          messages << message
        end
        return messages
      end
      
      def peek(queue_name, options = {})
        return get_messages(queue_name, {:peek_only => true}.merge(options))
      end
      
      def delete_message(queue_name, message_id, pop_receipt)
        url = generate_request_uri("#{queue_name}/messages/#{message_id}", :pop_receipt => pop_receipt)
        request = generate_request("DELETE", url)
        request.execute()
      end
      
      def clear_queue(queue_name)
        url = generate_request_uri("#{queue_name}/messages")
        request = generate_request("DELETE", url)
        request.execute()
      end
    end
  end
end