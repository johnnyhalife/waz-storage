module WAZ
  module Tables
    # This exception is raised while trying to create table that already exists.
    class TableAlreadyExists < WAZ::Storage::StorageException
      def initialize(name)
        super("The table #{name} already exists on your account.")
      end
    end

    # This exception is raised while trying to delete an unexisting table.    
    class TableDoesNotExist < WAZ::Storage::StorageException
      def initialize(name)
        super("The specified table #{name} does not exist.")
      end
    end
  end
end