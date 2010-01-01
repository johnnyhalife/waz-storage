module WAZ
  module Tables
    # This is internally used by the waz-tables part of the gem and it exposes the Windows Azure Blob API REST methods 
    # implementation. You can use this class to perform an specific operation that isn't provided by the current API.
    class Service
      include WAZ::Storage::SharedKeyCoreService

      # Creates a table on the current Windows Azure Storage account.
      def create_table(table_name)
        raise WAZ::Storage::InvalidParameterValue, {:name => table_name, :values => ["must start with at least one lower/upper characted, can have character or any digit starting from the second position, must be from 3 through 63 characters long"]} unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        
        payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?><entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\"><title /><updated>#{Time.now.utc.iso8601}</updated><author><name/></author><id/><content type=\"application/xml\"><m:properties><d:TableName>#{table_name}</d:TableName></m:properties></content></entry>"

        begin
          execute :post, 'Tables', {}, { 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }, payload
          {:name => table_name, :url => "#{self.base_url}/Tables('#{table_name}"}
        rescue RestClient::RequestFailed
          raise WAZ::Tables::TableAlreadyExists, table_name if $!.http_code == 409
        end
      end
      
      # Delete a table on the current Windows Azure Storage account.
      def delete_table(table_name)
        raise WAZ::Storage::InvalidParameterValue, {:name => table_name, :values => ["must start with at least one lower/upper characted, can have character or any digit starting from the second position, must be from 3 through 63 characters long"]} unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        begin
          execute :delete, "Tables('#{table_name}')", {}, { 'Date' => Time.new.httpdate,  'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }
        rescue RestClient::ResourceNotFound
          raise WAZ::Tables::TableDoesNotExist, table_name if $!.http_code == 404
        end
      end
      
      # Lists all existing tables on the current storage account.
      def list_tables(next_table_name = nil)
        query = { 'NextTableName' => next_table_name } unless next_table_name.nil?
        content = execute :get, "Tables", query ||= {}, { 'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }

        doc = REXML::Document.new(content)
        tables = REXML::XPath.each(doc, '/feed/entry').map do |item|
            { :name => REXML::XPath.first(item.elements['content'], "m:properties/d:TableName", {"m" => "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata", "d" => "http://schemas.microsoft.com/ado/2007/08/dataservices"}).text,
              :url => REXML::XPath.first(item, "id").text }
        end
        
        return tables, content.headers[:x_ms_continuation_nexttablename]
      end
      
      # Retrieves an existing table on the current storage account.
      def get_table(table_name)
        raise WAZ::Storage::InvalidParameterValue, {:name => table_name, :values => ["must start with at least one lower/upper characted, can have character or any digit starting from the second position, must be from 3 through 63 characters long"]} unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)        
        
        begin
          content = execute :get, "Tables('#{table_name}')", {}, { 'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }
          doc = REXML::Document.new(content)
          item = REXML::XPath.first(doc, "entry")
          return {  :name => REXML::XPath.first(item.elements['content'], "m:properties/d:TableName", {"m" => "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata", "d" => "http://schemas.microsoft.com/ado/2007/08/dataservices"}).text,
                    :url => REXML::XPath.first(item, "id").text }
        rescue RestClient::ResourceNotFound
          raise WAZ::Tables::TableDoesNotExist, table_name if $!.http_code == 404
        end        
      end
      
      # Retrieves an existing entity on the current storage account.
      def get_entity(table_name, partition_key, row_key)
        raise WAZ::Storage::InvalidParameterValue, {:name => table_name, :values => ["must start with at least one lower/upper characted, can have character or any digit starting from the second position, must be from 3 through 63 characters long"]} unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        
        begin
          parse_entity_response execute :get, "#{table_name}(PartitionKey='#{partition_key}',RowKey='#{row_key}')", {}, { 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }
        rescue
          puts $!
        end
      end    
        
      # Insert a new entity on the provided table for the current storage account
      # TODO: catch all api errors as described on Table Service Error Codes on MSDN (http://msdn.microsoft.com/en-us/library/dd179438.aspx)
      # TODO: parse the response and return the inserted entity      
      def insert_entity(table_name, entity)
        raise WAZ::Storage::InvalidParameterValue, {:name => table_name, :values => ["must start with at least one lower/upper characted, can have character or any digit starting from the second position, must be from 3 through 63 characters long"]} unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        raise WAZ::Tables::TooManyProperties, entity[:fields].length if entity[:fields].length > 252 
        
        payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>" \
                   "<entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\">" \
                   "<id>#{self.use_ssl ? 'https://' : 'http://'}#{self.base_url}/#{table_name}(PartitionKey='#{entity[:partition_key]}',RowKey='#{entity[:row_key]}')</id>" \
                   "<title /><updated>#{Time.now.utc.iso8601}</updated><author><name /></author><link rel=\"edit\" title=\"#{table_name}\" href=\"#{table_name}(PartitionKey='#{entity[:partition_key]}',RowKey='#{entity[:row_key]}')\" /><content type=\"application/xml\"><m:properties>"

        entity[:fields].each { |f| payload << (!f[:value].nil? ? "<d:#{f[:name]} m:type=\"Edm.#{f[:type]}\">#{f[:value].to_s}</d:#{f[:name]}>" : "<d:#{f[:name]} m:type=\"Edm.#{f[:type]}\" m:null=\"true\" />") }        

        payload << "<d:PartitionKey>#{entity[:partition_key]}</d:PartitionKey>" \
                   "<d:RowKey>#{entity[:row_key]}</d:RowKey>" \
                   "<d:Timestamp m:type=\"Edm.DateTime\">#{Time.now.utc.iso8601}</d:Timestamp></m:properties></content></entry>"
        begin
          execute :post, table_name, {}, { 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }, payload
        rescue RestClient::RequestFailed          
          raise WAZ::Tables::EntityAlreadyExists, entity[:row_key] if $!.http_code == 409 and $!.response.body.include?('EntityAlreadyExists')          
        end     
      end
      
      # Delete an existing entity in a table.
      def delete_entity(table_name, partition_key, row_key)
        raise WAZ::Storage::InvalidParameterValue, {:name => table_name, :values => ["lower letters, numbers or - (hypen), and must not start or end with - (hyphen)"]} unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        
        begin
          execute :delete, "#{table_name}(PartitionKey='#{partition_key}',RowKey='#{row_key}')", {}, { 'If-Match' => '*', 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }
        rescue RestClient::ResourceNotFound
          raise WAZ::Tables::TableDoesNotExist, table_name if $!.http_code == 404 and $!.response.body.include?('TableNotFound')
          raise WAZ::Tables::EntityDoesNotExist, "(PartitionKey='#{partition_key}',RowKey='#{row_key}')" if $!.http_code == 404
        end
      end
      
      private
        def parse_entity_response(response)
          File.open('/Users/jpgarcia/Desktop/entity.txt','w') { |f| f.write response}
          doc = REXML::Document.new(response)
          table_name = REXML::XPath.first(doc, "/entry/link").attributes['title']
          entry = REXML::XPath.first(doc, "/entry")
          partition_key = REXML::XPath.first(entry.elements['content'], "m:properties/d:PartitionKey", {"m" => "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata", "d" => "http://schemas.microsoft.com/ado/2007/08/dataservices"}).text
          row_key = REXML::XPath.first(entry.elements['content'], "m:properties/d:RowKey", {"m" => "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata", "d" => "http://schemas.microsoft.com/ado/2007/08/dataservices"}).text
          url = REXML::XPath.first(doc, "/entry/id").text 

          fields = REXML::XPath.each(entry.elements['content'], 'm:properties/*', {"m" => "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"}).map do |item|
            { :name => item.name, 
              :type => item.attributes['m:type'].nil? ? 'String' : item.attributes['m:type'].gsub('Edm.',''), 
              :value => parse_value(item) }
          end
          
          fields = fields.reject{|item| item[:name] == 'PartitionKey' or item[:name] == 'RowKey'}
          { :table_name => table_name, :partition_key => partition_key, :row_key => row_key , :url => url, :fields => fields}          
        end
        
      private
        def parse_value(item)    
          return nil if !item.attributes['m:null'].nil? and item.attributes['m:null'] == 'true'
          case item.attributes['m:type']
            when 'Edm.Int16'
              item.text.to_i
            when 'Edm.Int32'
              item.text.to_i
            when 'Edm.Int64'
              item.text.to_i
            when 'Edm.Boolean'
              item.text == 'true'
            when 'Edm.Single'
              item.text.to_f
            when 'Edm.Double'
              item.text.to_f
            else
              item.text
          end
        end
    end
  end
end