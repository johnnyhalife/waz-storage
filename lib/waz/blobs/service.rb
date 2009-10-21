module WAZ
  module Blobs
    # This is internally used by the waz-blobs part of the gem and it exposes the Windows Azure Blob API REST methods 
    # implementation. You can use this class to perform an specific operation that isn't provided by the current API.
    class Service
      include WAZ::Storage::SharedKeyCoreService
      
      # Creates a container on the current Windows Azure Storage account.
      def create_container(container_name)
        execute :put, container_name
      end
      
      # Retrieves all the properties existing on the container.
      def get_container_properties(container_name)
        execute(:get, container_name).headers
      end
      
      # Set the container properties (metadata). 
      #
      # Remember that custom properties should be named as :x_ms_meta_{propertyName} in order
      # to have Windows Azure to persist them.
      def set_container_properties(container_name, properties = {})
        execute :put, container_name, { :comp => 'metadata' }, properties
      end
      
      # Retrieves the value of the :x_ms_prop_publicaccess header from the
      # container properties indicating whether the container is publicly 
      # accessible or not.
      def get_container_acl(container_name)
        headers = execute(:get, container_name, { :comp => 'acl' }).headers
        headers[:x_ms_prop_publicaccess].downcase == true.to_s
      end

      # Sets the value of the :x_ms_prop_publicaccess header from the
      # container properties indicating whether the container is publicly 
      # accessible or not.
      #
      # Default is _false_
      def set_container_acl(container_name, public_available = false)
        execute :put, container_name, { :comp => 'acl' }, { :x_ms_prop_publicaccess => public_available.to_s }
      end

      # Lists all the containers existing on the current storage account.
      def list_containers(options = {})
        content = execute(:get, nil, options.merge(:comp => 'list'))
        doc = REXML::Document.new(content)
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
        execute :delete, container_name
      end

      # Lists all the blobs inside the given container.
      def list_blobs(container_name)
        content = execute(:get, container_name, { :comp => 'list' })
        doc = REXML::Document.new(content)
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
        execute :put, path, nil, metadata.merge("Content-Type" => content_type), payload
      end
      
      # Retrieves a blob (content + headers) from the current path.
      def get_blob(path)
        execute :get, path 
      end

      # Deletes the blob existing on the current path.
      def delete_blob(path)
        execute :delete, path
      end
            
      # Retrieves the properties associated with the blob at the given path.
      def get_blob_properties(path)
        execute(:head, path).headers
      end

      # Sets the properties (metadata) associated to the blob at given path.
      def set_blob_properties(path, properties ={})
        execute :put, path, { :comp => 'metadata' }, properties
      end
      
      # Copies a blob within the same account (not necessarily to the same container)
      def copy_blob(source_path, dest_path)
        execute :put, dest_path, nil, { :x_ms_version => "2009-04-14", :x_ms_copy_source => canonicalize_message(source_path) }
      end
    end
  end
end