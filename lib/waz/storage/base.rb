module WAZ
	module Storage
    # This class is used to handle a general connection with Windows Azure Storage Account and it 
    # should be used at least once on the application bootstrap or configuration file.
    # 
    # The usage is pretty simple as it's depicted on the following sample
    #  WAZ::Storage::establish_connection!(:account_name => "my_account_name", 
    #                                      :access_key => "your_base64_key", 
    #                                      :use_ssl => false)
    #
		class Base
			class << self
				# Sets the basic configuration parameters to use the API on the current context
				# required parameters are :account_name, :access_key. 
        #
				# All other parameters are optional.
				def establish_connection!(options = {})
					raise InvalidOption, :account_name unless options.keys.include? :account_name
					raise InvalidOption, :access_key unless options.keys.include? :access_key
					options[:use_ssl] = false unless options.keys.include? :use_ssl
					(@connections ||= []) << options
				end
				
        # Block Syntax
        #
        #  Pushes the named repository onto the context-stack,
        #  yields a new session, and pops the context-stack.
        #
        #  This helps you set contextual operations like in the following sample
        #  
        #  Base.establish_connection(options) do
        #   # do some operations on the given context
        #  end
        #  
        #  The context is restored to the previous one (what you configured on establish_connection!)
        #  or nil.
        #
        # Non-Block Syntax
        #
        #  Behaves exactly as establish_connection!
				def establish_connection(options = {}) # :yields: current_context
			    establish_connection!(options)
			    if (block_given?)
  				  begin 
  				    return yield
  				  ensure
  				    @connections.pop() if connected?
  				  end
  				end
				end
				
				# Returns the default connection (last set) parameters. It will raise an exception WAZ::Storage::NotConnected
				# when there's no connection information registered.
				def default_connection
          raise NotConnected unless connected?
					return @connections.last
				end
				
				# Returns a value indicating whether the current connection information has been set or not.
				def connected?
					return false if (@connections.nil?)
					return false if (@connections.empty?)
					return true
				end
			end
		end
	end
end