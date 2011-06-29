module WAZ
  module Storage
    # This module is imported by the specific services that use Shared Key authentication profile. On the current implementation
    # this module is imported from WAZ::Queues::Service and WAZ::Blobs::Service.
    module SharedKeyCoreService
      attr_accessor :account_name, :access_key, :use_ssl, :base_url, :type_of_service, :use_devenv, :use_sas_auth_only, :sharedaccesssignature
      
      # Creates an instance of the implementor service (internally used by the API).
      def initialize(options = {})
        # Flag to define the use of shared access signature only
        self.use_sas_auth_only = options[:use_sas_auth_only] or false
        self.sharedaccesssignature = options[:sharedaccesssignature] 

        self.account_name = options[:account_name]
        self.access_key = options[:access_key]
        self.type_of_service = options[:type_of_service]        
        self.use_ssl = options[:use_ssl] or false
        self.use_devenv = !!options[:use_devenv]
        self.base_url = "#{options[:type_of_service] or "blobs"}.#{options[:base_url] or "core.windows.net"}" unless self.use_devenv
        self.base_url ||= (options[:base_url] or "core.windows.net") 
      end
      
      # Generates a request based on Adam Wiggings' rest-client, including all the required headers
      # for interacting with Windows Azure Storage API (except for Tables). This methods embeds the 
      # authorization key signature on the request based on the given access_key.
      def generate_request(verb, url, headers = {}, payload = nil)
        http_headers = {}
        headers.each{ |k, v| http_headers[k.to_s.gsub(/_/, '-')] = v} unless headers.nil?
        http_headers.merge!("x-ms-Date" => Time.new.httpdate)
        http_headers.merge!("Content-Length" => (payload or "").size)
        request = {:headers => http_headers, :method => verb.to_s.downcase.to_sym, :url => url, :payload => payload}
        request[:headers].merge!("Authorization" => "SharedKey #{account_name}:#{generate_signature(request)}") unless self.use_sas_auth_only 
        return RestClient::Request.new(request)
      end
      
      # Generates the request uri based on the resource path, the protocol, the account name and the parameters passed
      # on the options hash. 
      def generate_request_uri(path = nil, options = {})
        protocol = use_ssl ? "https" : "http"
        query_params = options.keys.sort{ |a, b| a.to_s <=> b.to_s}.map{ |k| "#{k.to_s.gsub(/_/, '')}=#{CGI.escape(options[k].to_s)}"}.join("&") unless options.nil? or options.empty?
        uri = "#{protocol}://#{base_url}/#{path.start_with?(account_name) ? "" : account_name }#{((path or "").start_with?("/") or path.start_with?(account_name)) ? "" : "/"}#{(path or "")}" if !self.use_devenv.nil? and self.use_devenv
        uri ||= "#{protocol}://#{account_name}.#{base_url}#{(path or "").start_with?("/") ? "" : "/"}#{(path or "")}" 
        if self.use_sas_auth_only 
          uri << "?#{self.sharedaccesssignature.gsub(/\?/,'')}" 
	else
          uri << "?#{query_params}" if query_params
        end        
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
        comp_component = url.scan(/comp=[^&]+/i).first()
        uri_component << "?#{comp_component}" if comp_component
        canonicalized_message = "/#{self.account_name}/#{uri_component}"
        return canonicalized_message
      end
      
      # Generates the signature based on Micosoft specs for the REST API. It includes some special headers, 
      # the canonicalized header line and the canonical form of the message, all of the joined by \n character. Encoded with 
      # Base64 and encrypted with SHA256 using the access_key as the seed.
      def generate_signature(options = {})
        return generate_signature20090919(options) if options[:headers]["x-ms-version"] == "2009-09-19"

        signature = options[:method].to_s.upcase + "\x0A" +
                     (options[:headers]["Content-MD5"] or "") + "\x0A" +
                     (options[:headers]["Content-Type"] or "") + "\x0A" +
                     (options[:headers]["Date"] or "")+ "\x0A"

        signature += canonicalize_headers(options[:headers]) + "\x0A" unless self.type_of_service == 'table'
        signature += canonicalize_message(options[:url])
        signature = signature.toutf8 if(signature.respond_to? :toutf8)
        Base64.encode64(HMAC::SHA256.new(Base64.decode64(self.access_key)).update(signature).digest)
      end

      def generate_signature20090919(options = {})
        signature = options[:method].to_s.upcase + "\x0A" +
                    (options[:headers]["Content-Encoding"] or "") + "\x0A" +
                    (options[:headers]["Content-Language"] or "") + "\x0A" +
                    (options[:headers]["Content-Length"] or "").to_s + "\x0A" +                    
                    (options[:headers]["Content-MD5"] or "") + "\x0A" +
                    (options[:headers]["Content-Type"] or "") + "\x0A" +
                    (options[:headers]["Date"] or "")+ "\x0A" +
                    (options[:headers]["If-Modified-Since"] or "")+ "\x0A" +
                    (options[:headers]["If-Match"] or "")+ "\x0A" +
                    (options[:headers]["If-None-Match"] or "")+ "\x0A" +                    
                    (options[:headers]["If-Unmodified-Since"] or "")+ "\x0A" +
                    (options[:headers]["Range"] or "")+ "\x0A" +                    
                    canonicalize_headers(options[:headers]) + "\x0A" +
                    canonicalize_message20090919(options[:url])

        signature = signature.toutf8 if(signature.respond_to? :toutf8)
        Base64.encode64(HMAC::SHA256.new(Base64.decode64(self.access_key)).update(signature).digest)
      end
      
      def canonicalize_message20090919(url)
        uri_component = url.gsub(/https?:\/\/[^\/]+\//i, '').gsub(/\?.*/i, '')
        query_component = (url.scan(/\?(.*)/i).first() or []).first()
        query_component = query_component.split('&').sort{|a, b| a <=> b}.map{ |p| CGI::unescape(p.split('=').join(':')) }.join("\n") if query_component
        canonicalized_message = "/#{self.account_name}/#{uri_component}"
        canonicalized_message << "\n#{query_component}" if query_component
        return canonicalized_message
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
