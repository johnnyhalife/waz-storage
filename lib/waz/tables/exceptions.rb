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
    
    # This exception is raised when provided more than the 252 properties allowed by the Rest API.
    class TooManyProperties < WAZ::Storage::StorageException
      def initialize(total)
        super("The entity contains more properties than allowed (252). The entity has #{total} properties.")
      end
    end

    # This exception is raised when the specified entity already exists.
    class EntityAlreadyExists < WAZ::Storage::StorageException
      def initialize(row_key)
        super("The specified entity already exists. RowKey: #{row_key}")
      end
    end
    
    # This exception is raised while trying to delete an unexisting entity.    
    class EntityDoesNotExist < WAZ::Storage::StorageException
      def initialize(key)
        super("The specified entity with #{key} does not exist.")
      end
    end
  end
end