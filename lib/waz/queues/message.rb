module WAZ
  module Queues
    # This class is used to model a Message inside Windows Azure Queues the usage
    # it's pretty simple, here you can see a couple of samples. Messages consist on an UTF-8 string up-to 8KB and some metadata
    # regarding its status and visibility. Here are all the things that you can do with a message:
    #
    #  message.message_id #=> returns message id
    #
  	#  # this is the most important method regarding messages
  	#  message.message_text #=> returns message contents
    #  
  	#  message.pop_receipt #=> used for correlating your dequeue request + a delete operation
    #  
  	#  message.expiration_time #=> returns when the message will be removed from the queue
    #
  	#  message.time_next_visible #=> when the message will be visible to other users
    #
  	#  message.insertion_time #=> when the message will be visible to other users
    #
  	#  message.queue_name #=> returns the queue name where the message belongs
    #
  	#  # remove the message from the queue
  	#  message.destroy! 
  	#
    class Message      
      class << self
        # This method is internally used by this class. It's the way we keep a single instance of the 
        # service that wraps the calls the Windows Azure Queues API. It's initialized with the values
        # from the default_connection on WAZ::Storage::Base initialized thru establish_connection!
        def service_instance
          options = WAZ::Storage::Base.default_connection.merge(:type_of_service => "queue")
          (@service_instances ||= {})[options[:account_name]] ||= Service.new(options)
        end
      end
      
      attr_accessor :message_id, :message_text, :pop_receipt, :expiration_time, :insertion_time, :time_next_visible, :dequeue_count
       
      # Creates an instance of Message class, this method is intended to be used internally from the 
      # Queue.
      def initialize(params = {})
        self.message_id = params[:message_id]
        self.message_text = params[:message_text]
        self.pop_receipt = params[:pop_receipt] 
        self.expiration_time = params[:expiration_time]
        self.insertion_time = params[:insertion_time]
        self.time_next_visible = params[:time_next_visible]
        self.dequeue_count = params[:dequeue_count]
        @queue_name = params[:queue_name]
      end
      
      # Returns the Queue name where the Message belongs to
      def queue_name
        return @queue_name
      end
      
      # Marks the message for deletion (to later be removed from the queue by the garbage collector). If the message
      # where the message is being actually called was peeked from the queue instead of locked it will raise the 
      # WAZ::Queues:InvalidOperation exception since it's not a permited operation.
      def destroy!
        raise WAZ::Queues::InvalidOperation if pop_receipt.nil?
        self.class.service_instance.delete_message(queue_name, message_id, pop_receipt)
      end            
    end
  end
end