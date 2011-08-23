module WAZ
  module Blobs
    # This class is used to model the Container inside Windows Azure Blobs the usage
    # it's pretty simple, here you can see a couple of samples. Here you can find Microsoft's REST API description available on MSDN 
    # at http://msdn.microsoft.com/en-us/library/dd179361.aspx
    #
    #  # list available containers
  	#  WAZ::Blobs::Container.list
    #
  	#  # create a container
  	#  WAZ::Blobs::Container.create('my-container')
    #
  	#  # get a specific container 
  	#  my_container = WAZ::Blobs::Container.find('my-container')
    #
  	#  # get container properties (including default headers)
  	#  my_container.metadata #=> hash containing beautified metadata (:x_ms_meta_name)
    #
  	#  # set container properties (should follow x-ms-meta to be persisted)
  	#  my_container.put_properties(:x_ms_meta_MyProperty => "my value")
    #
  	#  # get a the value indicating whether the container is public or not
  	#  my_container.public_access? #=> true or false based on x-ms-prop-publicaccess
  	#
  	#  # set a value indicating whether the container is public or not
  	#  my_container.public_access = false
    #
  	#  # delete container
  	#  my_container.destroy!
  	#
  	#  # store a blob on the given container
  	#  my_container.store('my-blob', blob_content, 'application/xml')
  	#
  	#  # retrieve a particular blob from a container
    #  my_container['my-blob']
    #
  	#  # retrieve a blob list from a container
  	#  my_container.blobs #=> WAZ::Blobs::BlobObject collection
  	#
    class Container
      class << self 
        # Creates a new container with the given name.
        def create(name)
          raise WAZ::Storage::InvalidParameterValue, {:name => "name", :values => ["lower letters, numbers or - (hypen), and must not start or end with - (hyphen)"]} unless WAZ::Storage::ValidationRules.valid_name?(name)
          service_instance.create_container(name)
          return Container.new(:name => name)
        end
        
        # Finds a container by name. It will return nil if no container was found.
        def find(name)
          begin 
            properties = service_instance.get_container_properties(name)
            return Container.new(properties.merge(:name => name))
          rescue RestClient::ResourceNotFound
            return nil
          end
        end
        
        # Returns all the containers on the given account.
        def list(options = {})
          service_instance.list_containers(options).map { |container| Container.new(container) }
        end
        
        # This method is internally used by this class. It's the way we keep a single instance of the 
        # service that wraps the calls the Windows Azure Blobs API. It's initialized with the values
        # from the default_connection on WAZ::Storage::Base initialized thru establish_connection!
        def service_instance
          options = WAZ::Storage::Base.default_connection.merge(:type_of_service => "blob")
          (@service_instances ||= {})[options[:account_name]] ||= Service.new(options)
        end        
      end
      
      attr_accessor :name
      
      # Creates a new instance of the WAZ::Blobs::Container. This class isn't intended for external use
      # to access or create a container you should use the class methods provided like list, create, or find.
      def initialize(options = {})
        raise WAZ::Storage::InvalidOption, :name unless options.keys.include?(:name) and !options[:name].empty?
        self.name = options[:name]
      end
      
      # Returns the container metadata.
      def metadata
        self.class.service_instance.get_container_properties(self.name)
      end
      
      # Adds metadata for the container.Those properties are sent as HTTP Headers it's really important that you name your custom 
      # properties with the <em>x-ms-meta</em> prefix, if not the won't be persisted by the Windows Azure Blob Storage API.
      def put_properties!(properties = {})
        self.class.service_instance.set_container_properties(self.name, properties)
      end
      
      # Removes the container from the current account.
      def destroy!
        self.class.service_instance.delete_container(self.name)
      end
      
      # Retuns a value indicating whether the container is public accessible (i.e. from a Web Browser) or not.
      def public_access?
        self.class.service_instance.get_container_acl(self.name)
      end
      
      # Sets a value indicating whether the container is public accessible (i.e. from a Web Browser) or not.  
      def public_access=(value)
        self.class.service_instance.set_container_acl(self.name, value)
      end
      
      # Returns a list of blobs (WAZ::Blobs::BlobObject) contained on the current container.
      def blobs
        self.class.service_instance.list_blobs(name).map { |blob| WAZ::Blobs::BlobObject.new(blob) }
      end
      
      # Stores a blob on the container with under the given name, with the given content and 
      # the required <em>content_type</em>. <strong>The <em>options</em> parameters if provided
      # will set the default metadata properties for the blob</strong>.
      def store(blob_name, payload, content_type, options = {})
        blob_name.gsub!(%r{^/}, '')
        self.class.service_instance.put_blob("#{self.name}/#{blob_name}", payload, content_type, options)
        return BlobObject.new(:name => blob_name, 
                              :url => self.class.service_instance.generate_request_uri("#{self.name}/#{blob_name}"),
                              :content_type => content_type)
      end

      # Uploads the contents of a stream to the specified blob within this container, using
      # the required <em>content_type</em>. The stream will be uploaded in blocks of size
      # <em>block_size</em> bytes, which defaults to four megabytes. <strong>The <em>options</em>
      # parameter, if provided, will set the default metadata properties for the blob</strong>.
      def upload(blob_name, stream, content_type, options = {}, block_size = 4 * 2**20)
        blob_name.gsub!(%r{^/}, '')
        path = "#{self.name}/#{blob_name}"
        n = 0
        until stream.eof?
          self.class.service_instance.put_block path, Base64.encode64('%064d' % n), stream.read(block_size)
          n += 1
        end
        self.class.service_instance.put_block_list path, (0...n).map{|id| Base64.encode64('%064d' % id)}, content_type, options
        return BlobObject.new(:name => blob_name, :url => self.class.service_instance.generate_request_uri("#{self.name}/#{blob_name}"), :content_type => content_type)
      end
      
      # Retrieves the blob by the given name. If the blob isn't found on the current container
      # it will return nil instead of throwing an exception.
      def [](blob_name)
        begin
          blob_name.gsub!(%r{^/}, '')
          properties = self.class.service_instance.get_blob_properties("#{self.name}/#{blob_name}")
          return BlobObject.new(:name => blob_name, 
                                :url => self.class.service_instance.generate_request_uri("#{self.name}/#{blob_name}"),
                                :content_type => properties[:content_type])
        rescue RestClient::ResourceNotFound
          return nil
        end
      end
    end
    class BlobSecurity
      Container = 'container'
      Blob = 'blob'
      Private = ''
    end
  end
end
