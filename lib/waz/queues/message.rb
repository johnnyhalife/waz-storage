module WAZ
  module Queues
    class Message
      attr_accessor :message_id, :message_text, :pop_receipt, :expiration_time, :insertion_time, :time_next_visible
      def initialize(params = {})
        self.message_id = params[:message_id]
        self.message_text = params[:message_text]
        self.pop_receipt = params[:pop_receipt] 
        self.expiration_time = params[:expiration_time]
        self.insertion_time = params[:insertion_time]
        self.time_next_visible = params[:time_next_visible]
        @queue_name = params[:queue_name]
      end
      
      def queue_name
        return @queue_name
      end
      
      def destroy!
        raise WAZ::Queues::InvalidOperation if pop_receipt.nil?
        service_instance.delete_message(queue_name, message_id, pop_receipt)
      end
            
      private
        def service_instance
          options = WAZ::Storage::Base.default_connection
          @service_instance ||= Service.new(options[:account_name], options[:access_key], "queue")
        end
    end
  end
end