module WAZ
  module Blobs
    class Container
      class << self 
        def create(name)
          service_instance.create_container(name)
          return Container.new(:name => name)
        end
        
        def find(name)
          begin 
            properties = service_instance.get_container_properties(name)
            return Container.new(properties.merge(:name => name))
          rescue RestClient::ResourceNotFound
            return nil
          end
        end
        
        def list(options = {})
          service_instance.list_containers(options).map { |container| Container.new(container) }
        end
        
        private
          def service_instance
            options = WAZ::Storage::Base.default_connection
            return Service.new(options[:account_name], options[:access_key], "blob")
          end        
      end
      
      attr_accessor :name, :properties, :public_access

      def initialize(options = {})
        raise WAZ::Storage::InvalidOption, :name unless options.keys.include?(:name) and !options[:name].empty?
        self.name = options[:name]
        self.properties = options
      end
      
      def metadata
        service_instance.get_container_properties(self.name)
      end
      
      def put_properties!(properties = {})
        service_instance.set_container_properties(self.name, properties)
      end
      
      def destroy!
        service_instance.delete_container(self.name)
      end
      
      def public_access?
        public_access ||= service_instance.get_container_acl(self.name)
      end
      
      def public_access=(value)
        public_access = value
        service_instance.set_container_acl(self.name, value)
      end
      
      def blobs
        service_instance.list_blobs(name).map { |blob| WAZ::Blobs::BlobObject.new(blob) }
      end
      
      def store(blob_name, payload, content_type, options = {})
        service_instance.put_blob("#{self.name}/#{blob_name}", payload, content_type, options)
        return BlobObject.new(:name => blob_name, 
                              :url => service_instance.generate_request_uri(nil, "#{self.name}/#{blob_name}"),
                              :content_type => content_type)
      end
      
      def [](blob_name)
        begin
          properties = service_instance.get_blob_properties("#{self.name}/#{blob_name}")
          return BlobObject.new(:name => blob_name, 
                                :url => service_instance.generate_request_uri(nil, "#{self.name}/#{blob_name}"),
                                :content_type => properties[:content_type])
        rescue RestClient::ResourceNotFound
          return nil
        end
      end
      
      # TODO: Is really this the best way of handling this scenario?
      private
        def service_instance
          options = WAZ::Storage::Base.default_connection
          @service_instance ||= Service.new(options[:account_name], options[:access_key], "blob")
        end
    end
  end
end