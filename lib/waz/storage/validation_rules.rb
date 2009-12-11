module WAZ
  module Storage
    class ValidationRules
      class << self
        # Validates that the Container/Queue name given matches with the requirements of Windows Azure. 
        #
        # -Container/Queue names must start with a letter or number, and can contain only letters, numbers, and the dash (-) character.
        # -Every dash (-) character must be immediately preceded and followed by a letter or number.
        # -All letters in a container name must be lowercase.
        # -Container/Queue names must be from 3 through 63 characters long.
        def valid_name?(name)
          name =~ /^[a-z0-9][a-z0-9\-]{1,}[^-]$/ && name.length < 64
        end
      end
    end
  end
end