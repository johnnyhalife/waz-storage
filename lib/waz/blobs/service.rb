module WAZ
  module Blobs
    class Service
      include WAZ::Storage::SharedKeyCoreService
            
      def create_container(container_name)
        url = generate_request_uri(nil, container_name)
        request = generate_request("PUT", url)
        request.execute()
      end
      
      def get_container_properties(container_name)
        url = generate_request_uri(nil, container_name)
        request = generate_request("GET", url)
        request.execute().headers
      end
      
      def set_container_properties(container_name, properties = {})
        url = generate_request_uri("metadata", container_name)
        request = generate_request("PUT", url, properties)
        request.execute()
      end
      
      def get_container_acl(container_name)
        url = generate_request_uri("acl", container_name)
        request = generate_request("GET", url)
        request.execute().headers[:x_ms_prop_publicaccess].downcase == true.to_s
      end

      def set_container_acl(container_name, public_available = false)
        url = generate_request_uri("acl", container_name)
        request = generate_request("PUT", url, "x-ms-prop-publicaccess" => public_available.to_s)
        request.execute()
      end

      def list_containers(options = {})
        url = generate_request_uri("list", nil, options)
        request = generate_request("GET", url)
        doc = REXML::Document.new(request.execute())
        containers = []
        REXML::XPath.each(doc, '//Container/') do |item|
          containers << { :name => REXML::XPath.first(item, "Name").text,
                          :url => REXML::XPath.first(item, "Url").text,
                          :last_modified => REXML::XPath.first(item, "LastModified").text}
        end
        return containers
      end

      def delete_container(container_name)
        url = generate_request_uri(nil, container_name)
        request = generate_request("DELETE", url)
        request.execute()
      end

      def list_blobs(container_name)
        url = generate_request_uri("list", container_name)
        request = generate_request("GET", url)
        doc = REXML::Document.new(request.execute())
        containers = []
        REXML::XPath.each(doc, '//Blob/') do |item|
          containers << { :name => REXML::XPath.first(item, "Name").text,
                          :url => REXML::XPath.first(item, "Url").text,
                          :content_type =>  REXML::XPath.first(item, "ContentType").text }
        end
        return containers
      end

      def put_blob(path, payload, content_type = "application/octet-stream", metadata = {})
        url = generate_request_uri(nil, path)
        request = generate_request("PUT", url, metadata.merge("Content-Type" => content_type), payload)
        request.execute()
      end
          
      def get_blob(path)
        url = generate_request_uri(nil, path)
        request = generate_request("GET", url)
        request.execute()
      end

      def delete_blob(path)
        url = generate_request_uri(nil, path)
        request = generate_request("DELETE", url)
        request.execute()
      end
            
      def get_blob_properties(path)
        url = generate_request_uri(nil, path)
        request = generate_request("HEAD", url)
        request.execute().headers
      end

      def set_blob_properties(path, properties ={})
        url = generate_request_uri("metadata", path)
        request = generate_request("PUT", url, properties)
        request.execute()
      end
    end
  end
end