module WAZ
  module Storage
    class StorageException < StandardError
    end
    
    class InvalidOption < StorageException
      def initialize(missing_option)
        super("You did not provide one of the required parameters. Please provide the #{missing_option}.")
      end
    end
  end
end
