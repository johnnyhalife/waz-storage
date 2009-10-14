module WAZ
  module Storage
    module SharedKeyCoreService
      attr_accessor :account_name, :account_key, :use_ssl, :base_url
      
      def initialize(account_name, account_key, type_of_service = "blobs", use_ssl = false, base_url = "core.windows.net" )
        self.account_name = account_name
        self.account_key = account_key 
        self.use_ssl = use_ssl
        self.base_url = "#{type_of_service}.#{base_url}"
      end
      
      def generate_request(verb, url, headers = {}, payload = nil)
        http_headers = {}
        headers.each{ |k, v| http_headers[k.to_s.gsub(/_/, '-')] = v} unless headers.nil?
        request = RestClient::Request.new(:method => verb.downcase.to_sym, :url => url, :headers => http_headers, :payload => payload)
        request.headers["x-ms-Date"] = Time.new.httpdate
        request.headers["Content-Length"] = (request.payload or "").length
        request.headers["Authorization"] = "SharedKey #{account_name}:#{generate_signature(request)}"
        return request
      end
      
      # TODO: operations should be included as options since they're querystring params =)      
      def generate_request_uri(operation = nil, path = nil, options = {})
        protocol = use_ssl ? "https" : "http"
        query_params = options.keys.sort{ |a, b| a.to_s <=> b.to_s}.map{ |k| "#{k.to_s.gsub(/_/, '')}=#{options[k]}"}.join("&") unless options.empty?
        uri = "#{protocol}://#{account_name}.#{base_url}#{(path or "").start_with?("/") ? "" : "/"}#{(path or "")}#{operation ? "?comp=" + operation : ""}"
        uri << "#{operation ? "&" : "?"}#{query_params}" if query_params
        return uri
      end
      
      def canonicalize_headers(headers)
        cannonicalized_headers = headers.keys.select {|h| h.to_s.start_with? 'x-ms'}.map{ |h| "#{h.downcase.strip}:#{headers[h].strip}" }.sort{ |a, b| a <=> b }.join("\x0A")
        return cannonicalized_headers
      end
      
      def canonicalize_message(url)
        uri_component = url.gsub(/https?:\/\/[^\/]+\//i, '').gsub(/\?.*/i, '')
        comp_component = url.scan(/(comp=[^&]+)/i).first()
        uri_component << "?#{comp_component}" if comp_component
        canonicalized_message = "/#{self.account_name}/#{uri_component}"
        return canonicalized_message
      end
      
      def generate_signature(request)
         signature = request.method.to_s.upcase + "\x0A" +
                     (request.headers["Content-MD5"] or "") + "\x0A" +
                     (request.headers["Content-Type"] or "") + "\x0A" +
                     (request.headers["Date"] or "")+ "\x0A" +
                     canonicalize_headers(request.headers) + "\x0A" +
                     canonicalize_message(request.url)
                     
         return Base64.encode64(HMAC::SHA256.new(Base64.decode64(self.account_key)).update(signature.toutf8).digest)
       end
    end
  end
end