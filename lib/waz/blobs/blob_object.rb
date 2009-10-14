module WAZ
  module Blobs
    class BlobObject      
      attr_accessor :name, :url, :content_type
      
      def initialize(options = {})
        raise WAZ::Storage::InvalidOption, :name unless options.keys.include?(:name) and !options[:name].empty?
        raise WAZ::Storage::InvalidOption, :url unless options.keys.include?(:url) and !options[:url].empty?
        raise WAZ::Storage::InvalidOption, :content_type unless options.keys.include?(:content_type) and !options[:content_type].empty?
        self.name = options[:name]
        self.url = options[:url]
        self.content_type = options[:content_type]
      end
      
      def metadata
        service_instance.get_blob_properties(path)
      end
      
      def value
        @value ||= service_instance.get_blob(path)
      end
      
      def value=(new_value)
        service_instance.put_blob(path, new_value, content_type, metadata)
        @value = new_value
      end

      def put_properties!(properties = {})
        service_instance.set_blob_properties(path, properties)
      end
      
      def destroy!
        service_instance.delete_blob(path)
      end
      
      def path
        url.gsub(/https?:\/\/[^\/]+\//i, '').scan(/([^&]+)/i).first().first()
      end
      
      private
        def service_instance
          options = WAZ::Storage::Base.default_connection
          @service_instance ||= Service.new(options[:account_name], options[:access_key], "blob")
        end
    end
  end
end
  