module WAZ
  module Queues    
    # This exception is raised while trying when calling WAZ::Queues::Queue.create('queue_name') and
    # the metadata existing on the Queues Storage subsytem on the cloud contains different metadata from 
    # the given one.
    class QueueAlreadyExists < WAZ::Storage::StorageException
      def initialize(name)
        super("The queue #{name} already exists on your account.")
      end
    end
    
    # This exception is raised when an initialization parameter of any of the WAZ::Queues classes falls of 
    # the specified values.
    class OptionOutOfRange < WAZ::Storage::StorageException
      def initialize(args = {})
        super("The #{args[:name]} parameter is out of range allowed values go from #{args[:min]} to  #{args[:max]}.")
      end
    end
    
    # This exception is raised when the user tries to perform a delete operation over a peeked message. Since
    # peeked messages cannot by deleted given the fact that there's no pop_receipt associated with it 
    # this exception will be raised.
    class InvalidOperation < WAZ::Storage::StorageException
      def initialize()
        super("A peeked message cannot be delete, you need to lock it first (pop_receipt required).")
      end
    end
  end
end