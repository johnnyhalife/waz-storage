module WAZ
  module Storage
    # This module is imported by the specific services that use Shared Key authentication profile. On the current implementation
    # this module is imported from WAZ::Queues::Service and WAZ::Blobs::Service.
    module SharedKeyCoreService
      attr_accessor :account_name, :access_key, :use_ssl, :base_url
      
      # Creates an instance of the implementor service (internally used by the API).
      def initialize(options = {})
        self.account_name = options[:account_name]
        self.access_key = options[:access_key]
        self.use_ssl = options[:use_ssl] or false
        self.base_url = "#{options[:type_of_service] or "blobs"}.#{options[:base_url] or "core.windows.net"}"
      end
      
      # Generates a request based on Adam Wiggings' rest-client, including all the required headers
      # for interacting with Windows Azure Storage API (except for Tables). This methods embeds the 
      # authorization key signature on the request based on the given access_key.
      def generate_request(verb, url, headers = {}, payload = nil)
        http_headers = {}
        headers.each{ |k, v| http_headers[k.to_s.gsub(/_/, '-')] = v} unless headers.nil?
        request = RestClient::Request.new(:method => verb.to_s.downcase.to_sym, :url => url, :headers => http_headers, :payload => payload)
        request.headers["x-ms-Date"] = Time.new.httpdate
        request.headers["Content-Length"] = (request.payload or "").length
        request.headers["Authorization"] = "SharedKey #{account_name}:#{generate_signature(request)}"
        return request
      end
      
      # Generates the request uri based on the resource path, the protocol, the account name and the parameters passed
      # on the options hash. 
      def generate_request_uri(path = nil, options = {})
        protocol = use_ssl ? "https" : "http"
        query_params = options.keys.sort{ |a, b| a.to_s <=> b.to_s}.map{ |k| "#{k.to_s.gsub(/_/, '')}=#{options[k]}"}.join("&") unless options.nil? or options.empty?
        uri = "#{protocol}://#{account_name}.#{base_url}#{(path or "").start_with?("/") ? "" : "/"}#{(path or "")}"
        uri << "?#{query_params}" if query_params
        return uri
      end
      
      # Canonicalizes the request headers by following Microsoft's specification on how those headers have to be sorted 
      # and which of the given headers apply to be canonicalized.
      def canonicalize_headers(headers)
        cannonicalized_headers = headers.keys.select {|h| h.to_s.start_with? 'x-ms'}.map{ |h| "#{h.downcase.strip}:#{headers[h].strip}" }.sort{ |a, b| a <=> b }.join("\x0A")
        return cannonicalized_headers
      end
      
      # Creates a canonical representation of the message by combining account_name/resource_path.
      def canonicalize_message(url)
        uri_component = url.gsub(/https?:\/\/[^\/]+\//i, '').gsub(/\?.*/i, '')
        comp_component = url.scan(/(comp=[^&]+)/i).first()
        uri_component << "?#{comp_component}" if comp_component
        canonicalized_message = "/#{self.account_name}/#{uri_component}"
        return canonicalized_message
      end
      
      # Generates the signature based on Micosoft specs for the REST API. It includes some special headers, 
      # the canonicalized header line and the canonical form of the message, all of the joined by \n character. Encoded with 
      # Base64 and encrypted with SHA256 using the access_key as the seed.
      def generate_signature(request)
         signature = request.method.to_s.upcase + "\x0A" +
                     (request.headers["Content-MD5"] or "") + "\x0A" +
                     (request.headers["Content-Type"] or "") + "\x0A" +
                     (request.headers["Date"] or "")+ "\x0A" +
                     canonicalize_headers(request.headers) + "\x0A" +
                     canonicalize_message(request.url)
                     
         return Base64.encode64(HMAC::SHA256.new(Base64.decode64(self.access_key)).update(signature.toutf8).digest)
      end
      
      # Generates a Windows Azure Storage call, it internally calls url generation method
      # and the request generation message.
      def execute(verb, path, query = {}, headers = {}, payload = nil)
        url = generate_request_uri(path, query)
        request = generate_request(verb, url, headers, payload)
        request.execute()
      end
    end
  end
end