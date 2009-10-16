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
					@connection = options
				end
				
				# Returns the default connection (last set) parameters. It will raise an exception WAZ::Storage::NotConnected
				# when there's no connection information registered.
				def default_connection
          raise NotConnected unless connected?
					return @connection
				end
				
				# Returns a value indicating whether the current connection information has been set or not.
				def connected?
					return !@connection.nil?
				end
			end
		end
	end
end