module WAZ
  module Storage
    # This class is the base exception from where all the exceptions raised from this API 
    # inherit from. If you want to handle an exception that your code may throw and you don't
    # know which specific type you should handle, handle this type of exception.
    class StorageException < StandardError
    end
    
    # This exception raises whenever a required parameter for initializing any class isn't provided. From
    # WAZ::Storage::Base up to WAZ::Queues::Queue all of them use this exception.
    class InvalidOption < StorageException
      def initialize(missing_option)
        super("You did not provide one of the required parameters. Please provide the #{missing_option}.")
      end
    end
    
    # This exception is raised when the user tries to perform an operation on any Storage API class
    # without previously specificying the connection options.
    class NotConnected < StorageException
      def initialize
        super("You should establish connection before using the services, the connection configuration is required.")
      end
    end
    
    # This exception if raised when the value given for an argument doesn't fall into the permitted values. For example
    # if values on the blocklisttype aren't [all|uncommitted|committed]
    class InvalidParameterValue < StorageException
      def initialize(args = {})
        super("The value supplied for the parameter #{args[:name]} is invalid. Permitted values are #{args[:values].join(',')}")
      end
    end
  end
end
