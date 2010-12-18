%w{time cgi base64 rexml/document rexml/xpath restclient hmac-sha2 net/http}.each(&method(:require))
app_files = File.expand_path(File.join(File.dirname(__FILE__), 'waz', 'storage', '*.rb'))
Dir[app_files].each(&method(:load))

# It will depende on which version of Ruby (or if you have Rails) 
# but this method is required so we will add it the String class.
unless String.method_defined? :start_with?
  class String
    def start_with?(prefix)
      prefix = prefix.to_s
      self[0, prefix.length] == prefix
    end
  end
end

# The Merge method is not defined in the RFC 2616 
# and it's required to Merge entities in Windows Azure
module Net
  class HTTP < Protocol
    class Merge < HTTPRequest
      METHOD = 'MERGE'
      REQUEST_HAS_BODY  = true
      RESPONSE_HAS_BODY = false
    end
  end
end

# extendes the Symbol class to assign a type to an entity field
class Symbol
  attr_accessor :edm_type
end
