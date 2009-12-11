module WAZ
  module Blobs    
    # This exception is raised when the user tries to perform an operation over a snapshoted blob. Since
    # Snapshots are read-only copies of the original blob they cannot be modified
    class InvalidOperation < WAZ::Storage::StorageException
      def initialize()
        super("A snapshoted blob cannot be modified.")
      end
    end
  end
end