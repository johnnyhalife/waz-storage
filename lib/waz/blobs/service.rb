module WAZ
  module Blobs
    # This is internally used by the waz-blobs part of the gem and it exposes the Windows Azure Blob API REST methods 
    # implementation. You can use this class to perform an specific operation that isn't provided by the current API.
    class Service
      include WAZ::Storage::SharedKeyCoreService
      
      # Creates a container on the current Windows Azure Storage account.
      def create_container(container_name)
        url = generate_request_uri(container_name)
        request = generate_request("PUT", url)
        request.execute()
      end
      
      # Retrieves all the properties existing on the container.
      def get_container_properties(container_name)
        url = generate_request_uri(container_name)
        request = generate_request("GET", url)
        request.execute().headers
      end
      
      # Set the container properties (metadata). 
      #
      # Remember that custom properties should be named as :x_ms_meta_{propertyName} in order
      # to have Windows Azure to persist them.
      def set_container_properties(container_name, properties = {})
        url = generate_request_uri(container_name, :comp => 'metadata')
        request = generate_request("PUT", url, properties)
        request.execute()
      end
      
      # Retrieves the value of the :x_ms_prop_publicaccess header from the
      # container properties indicating whether the container is publicly 
      # accessible or not.
      def get_container_acl(container_name)
        url = generate_request_uri(container_name, :comp => 'acl')
        request = generate_request("GET", url)
        request.execute().headers[:x_ms_prop_publicaccess].downcase == true.to_s
      end

      # Sets the value of the :x_ms_prop_publicaccess header from the
      # container properties indicating whether the container is publicly 
      # accessible or not.
      #
      # Default is _false_
      def set_container_acl(container_name, public_available = false)
        url = generate_request_uri(container_name, :comp => 'acl')
        request = generate_request("PUT", url, "x-ms-prop-publicaccess" => public_available.to_s)
        request.execute()
      end

      # Lists all the containers existing on the current storage account.
      def list_containers(options = {})
        url = generate_request_uri( nil, options.merge(:comp => 'list'))
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

      # Deletes the given container from the Windows Azure Storage account.
      def delete_container(container_name)
        url = generate_request_uri(container_name)
        request = generate_request("DELETE", url)
        request.execute()
      end

      # Lists all the blobs inside the given container.
      def list_blobs(container_name)
        url = generate_request_uri(container_name, :comp => 'list')
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

      # Stores a blob on the given container.
      # 
      # Remarks path and payload are just text.
      # 
      # content_type is required by the blobs api, but on this method is defaulted to "application/octect-stream"
      #
      # metadata is a hash that stores all the properties that you want to add to the blob when creating it.
      def put_blob(path, payload, content_type = "application/octet-stream", metadata = {})
        url = generate_request_uri( path)
        request = generate_request("PUT", url, metadata.merge("Content-Type" => content_type), payload)
        request.execute()
      end
      
      # Retrieves a blob (content + headers) from the current path.
      def get_blob(path)
        url = generate_request_uri( path)
        request = generate_request("GET", url)
        request.execute()
      end

      # Deletes the blob existing on the current path.
      def delete_blob(path)
        url = generate_request_uri( path)
        request = generate_request("DELETE", url)
        request.execute()
      end
            
      # Retrieves the properties associated with the blob at the given path.
      def get_blob_properties(path)
        url = generate_request_uri( path)
        request = generate_request("HEAD", url)
        request.execute().headers
      end

      # Sets the properties (metadata) associated to the blob at given path.
      def set_blob_properties(path, properties ={})
        url = generate_request_uri( path, :comp => 'metadata')
        request = generate_request("PUT", url, properties)
        request.execute()
      end
    end
  end
end