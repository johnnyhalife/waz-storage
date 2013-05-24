module WAZ
  module Storage
    class ValidationRules
      class << self
        # Validates that the Container name given matches with the requirements of Windows Azure. 
        #
        # -Container names must start with a letter or number, and can contain only letters, numbers, and the dash (-) character.
        # -Every dash (-) character must be immediately preceded and followed by a letter or number.
        # -All letters in a container name must be lowercase.
        # -Container names must be from 3 through 63 characters long.
        # -An exception to the naming rules is made for the root container which may be addressed with the name "$root"
        def valid_container_name?(name)
          (name =~ /^[a-z0-9][a-z0-9\-]{1,}[^-]$/ && name.length < 64) || (name == "$root")
        end
        
        # Validates that the Queue name given matches with the requirements of Windows Azure. 
        #
        # -Queue names must start with a letter or number, and can contain only letters, numbers, and the dash (-) character.
        # -Every dash (-) character must be immediately preceded and followed by a letter or number.
        # -All letters in a container name must be lowercase.
        # -Queue names must be from 3 through 63 characters long.
        def valid_queue_name?(name)
          name =~ /^[a-z0-9][a-z0-9\-]{1,}[^-]$/ && name.length < 64
        end

        # Validates that the Table name given matches with the requirements of Windows Azure. 
        #
        # -Table names must start with at least one lower / upper character.
        # -Table names can have character or any digit starting from the second position.
        # -Table names must be from 3 through 63 characters long.
        def valid_table_name?(name)
          name =~ /^([a-z]|[A-Z]){1}([a-z]|[A-Z]|\d){2,62}$/
        end
      end
    end
  end
end
