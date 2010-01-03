module WAZ
  module Tables
    # This is internally used by the waz-tables part of the gem and it exposes the Windows Azure Blob API REST methods 
    # implementation. You can use this class to perform an specific operation that isn't provided by the current API.
    class Service
      include WAZ::Storage::SharedKeyCoreService
      
      DATASERVICES_NAMESPACE = "http://schemas.microsoft.com/ado/2007/08/dataservices"      
      DATASERVICES_METADATA_NAMESPACE = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"
      
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
            { :name => REXML::XPath.first(item.elements['content'], "m:properties/d:TableName", {"m" => DATASERVICES_METADATA_NAMESPACE, "d" => DATASERVICES_NAMESPACE}).text,
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
          return {  :name => REXML::XPath.first(item.elements['content'], "m:properties/d:TableName", {"m" => DATASERVICES_METADATA_NAMESPACE, "d" => DATASERVICES_NAMESPACE}).text,
                    :url => REXML::XPath.first(item, "id").text }
        rescue RestClient::ResourceNotFound
          raise WAZ::Tables::TableDoesNotExist, table_name if $!.http_code == 404
        end        
      end
             
      # Insert a new entity on the provided table for the current storage account
      # TODO: catch all api errors as described on Table Service Error Codes on MSDN (http://msdn.microsoft.com/en-us/library/dd179438.aspx)
      def insert_entity(table_name, entity)
        raise WAZ::Storage::InvalidParameterValue, {:name => table_name, :values => ["must start with at least one lower/upper characted, can have character or any digit starting from the second position, must be from 3 through 63 characters long"]} unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        raise WAZ::Tables::TooManyProperties, entity[:fields].length if entity[:fields].length > 252 
        
        begin
          parse_entity execute :post, table_name, {}, { 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }, generate_payload(table_name, entity)
        rescue RestClient::RequestFailed          
          raise WAZ::Tables::EntityAlreadyExists, entity[:row_key] if $!.http_code == 409 and $!.response.body.include?('EntityAlreadyExists')          
        end     
      end
      
      # Update an existing entity on the current storage account.
      # TODO: handle specific errors
      def update_entity(table_name, entity)
        raise WAZ::Storage::InvalidParameterValue, {:name => table_name, :values => ["must start with at least one lower/upper characted, can have character or any digit starting from the second position, must be from 3 through 63 characters long"]} unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        
        begin
          parse_entity execute :put, "#{table_name}(PartitionKey='#{entity[:partition_key]}',RowKey='#{entity[:row_key]}')", {}, { 'If-Match' => '*', 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' },  generate_payload(table_name, entity)
        rescue
          puts $!
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
      
      # Retrieves an existing entity on the current storage account.
      # TODO: handle specific errors
      def get_entity(table_name, partition_key, row_key)
        raise WAZ::Storage::InvalidParameterValue, {:name => table_name, :values => ["must start with at least one lower/upper characted, can have character or any digit starting from the second position, must be from 3 through 63 characters long"]} unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        
        begin
          parse_entity execute :get, "#{table_name}(PartitionKey='#{partition_key}',RowKey='#{row_key}')", {}, { 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }
        rescue
          puts $!
        end
      end    
      
      # Retrieves a set of entities on the current storage account for a given query.
      # When the :top => n is passed it returns only the first n rows that match with the query
      # TODO: handle specific errors
      def query_entity(table_name, expression, top = nil)
        raise WAZ::Storage::InvalidParameterValue, {:name => table_name, :values => ["must start with at least one lower/upper characted, can have character or any digit starting from the second position, must be from 3 through 63 characters long"]} unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        begin
          entities, next_partition_key, next_row_key = [], nil, nil
          begin
            query = {'$filter' => expression }
            query.merge!({ '$top' => top }) unless top.nil?
            query.merge!({ 'NextPartitionKey' => next_partition_key, 'NextRowKey' => next_row_key }) unless (next_partition_key.nil? and next_row_key.nil?)
            headers = { 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }
            response = execute :get, "#{table_name}()", query, headers
            next_partition_key, next_row_key  = response.headers[:x_ms_continuation_nextpartitionkey], response.headers[:x_ms_continuation_nextrowkey]
            entities << parse_entity(response)
            entities.flatten!
            break if (!top.nil? and entities.length == top)
          end while (!next_partition_key.nil? and !next_row_key.nil?)
          return entities
        rescue
          puts $!
        end
      end

      private 
        def generate_payload(table_name, entity)
          payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>" \
                     "<entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\">" \
                     "<id>#{generate_request_uri "#{table_name}"}(PartitionKey='#{entity[:partition_key]}',RowKey='#{entity[:row_key]}')</id>" \
                     "<title /><updated>#{Time.now.utc.iso8601}</updated><author><name /></author><link rel=\"edit\" title=\"#{table_name}\" href=\"#{table_name}(PartitionKey='#{entity[:partition_key]}',RowKey='#{entity[:row_key]}')\" /><content type=\"application/xml\"><m:properties>"

          entity[:fields].sort_by { |k| k }.each { |k,v| payload << (!v[:value].nil? ? "<d:#{k} m:type=\"Edm.#{v[:type]}\">#{v[:value].to_s}</d:#{k}>" : "<d:#{k} m:type=\"Edm.#{v[:type]}\" m:null=\"true\" />") }          

          payload << "<d:PartitionKey>#{entity[:partition_key]}</d:PartitionKey>" unless entity[:fields].keys.include?('PartitionKey') 
          payload << "<d:RowKey>#{entity[:row_key]}</d:RowKey>"  unless entity[:fields].keys.include?('RowKey') 
          payload << "<d:Timestamp m:type=\"Edm.DateTime\">#{Time.now.utc.iso8601}</d:Timestamp>" unless entity[:fields].keys.include?('TimeStamp')
          payload << "</m:properties></content></entry>"
          file = File.open '/Users/jpgarcia/Desktop/text.xml', 'w'
          file.write payload 
          return payload
        end

      private
        def parse_entity(response)
          doc = REXML::Document.new(response)
          xpath_query = REXML::XPath.first(doc, '/feed').nil? ? '/entry' : '/feed/entry' 
          entities = REXML::XPath.each(doc, xpath_query).map { |entry|
            table_name = REXML::XPath.first(entry, "link").attributes['title']
            etag = entry.attributes['m:etag']
            partition_key = REXML::XPath.first(entry.elements['content'], "m:properties/d:PartitionKey", {"m" => DATASERVICES_METADATA_NAMESPACE, "d" => DATASERVICES_NAMESPACE}).text
            row_key = REXML::XPath.first(entry.elements['content'], "m:properties/d:RowKey", {"m" => DATASERVICES_METADATA_NAMESPACE, "d" => DATASERVICES_NAMESPACE}).text
            url = REXML::XPath.first(entry, "id").text 

            fields = REXML::XPath.each(entry.elements['content'], 'm:properties/*', {"m" => DATASERVICES_METADATA_NAMESPACE}).map { |f|
              { f.name => { :type => f.attributes['m:type'].nil? ? 'String' : f.attributes['m:type'].gsub('Edm.',''), :value => parse_value(f) } }
            }
            fields = Hash[*fields.collect {|h| h.to_a}.flatten]
            { :table_name => table_name, :etag => etag, :partition_key => partition_key, :row_key => row_key , :url => url, :fields => fields}
          }
          (xpath_query == '/feed/entry') ? entities : entities.first unless entities.nil?
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