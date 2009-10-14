module WAZ
	module Storage
		# TODO: We should get better connection management
		class Base
			class << self
				def establish_connection!(options = {})
					raise InvalidOption.new(:account_name) unless options.keys.include? :account_name
					raise InvalidOption.new(:access_key) unless options.keys.include? :access_key
					options[:use_ssl] = false unless options.keys.include? :use_ssl
					@connection = options
				end
				
				def default_connection
					return @connection
				end
				
				def connected?
					return !@connection.nil?
				end
			end
		end
	end
end