module WAZ
  module Queues    
    class QueueAlreadyExists < WAZ::Storage::StorageException
      def initialize(name)
        super("The queue #{name} already exists on your account.")
      end
    end
    
    class OptionOutOfRange < WAZ::Storage::StorageException
      def initialize(args = {})
        super("The #{args[:name]} parameter is out of range allowed values go from #{args[:min]} to  #{args[:max]}.")
      end
    end
    
    class InvalidOperation < WAZ::Storage::StorageException
      def initialize()
        super("A peeked message cannot be delete, you need to lock it first (pop_receipt required).")
      end
    end
  end
end