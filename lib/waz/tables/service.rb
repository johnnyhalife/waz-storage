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
        raise WAZ::Tables::InvalidTableName, table_name unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)

        payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>" \
                  "<entry xmlns:d=\"#{DATASERVICES_NAMESPACE}\" xmlns:m=\"#{DATASERVICES_METADATA_NAMESPACE}\" xmlns=\"http://www.w3.org/2005/Atom\">" \
                  "<title /><updated>#{Time.now.utc.iso8601}</updated><author><name/></author><id/>" \
                  "<content type=\"application/xml\"><m:properties><d:TableName>#{table_name}</d:TableName></m:properties></content></entry>"

        begin
          execute :post, 'Tables', {}, default_headers, payload
          return {:name => table_name, :url => "#{self.base_url}/Tables('#{table_name}"}
        rescue RestClient::RequestFailed
          raise WAZ::Tables::TableAlreadyExists, table_name if $!.http_code == 409
        end
      end
      
      # Delete a table on the current Windows Azure Storage account.
      def delete_table(table_name)
        raise WAZ::Tables::InvalidTableName, table_name unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        begin
          execute :delete, "Tables('#{table_name}')", {}, default_headers
        rescue RestClient::ResourceNotFound
          raise WAZ::Tables::TableDoesNotExist, table_name if $!.http_code == 404
        end
      end
      
      # Lists all existing tables on the current storage account.
      # remove Content-Type if it's not working
      def list_tables(next_table_name = nil)
        query = { 'NextTableName' => next_table_name } unless next_table_name.nil?
        content = execute :get, "Tables", query ||= {}, default_headers

        doc = REXML::Document.new(content)
        tables = REXML::XPath.each(doc, '/feed/entry').map do |item|
            { :name => REXML::XPath.first(item.elements['content'], "m:properties/d:TableName", {"m" => DATASERVICES_METADATA_NAMESPACE, "d" => DATASERVICES_NAMESPACE}).text,
              :url => REXML::XPath.first(item, "id").text }
        end
        
        return tables, content.headers[:x_ms_continuation_nexttablename]
      end
      
      # Retrieves an existing table on the current storage account.
      # remove Content-Type if it's not working
      def get_table(table_name)
        raise WAZ::Tables::InvalidTableName, table_name unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)        
        
        begin
          content = execute :get, "Tables('#{table_name}')", {}, default_headers
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
        raise WAZ::Tables::InvalidTableName, table_name unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        raise WAZ::Tables::TooManyProperties, entity.length if entity.length > 252 
        
        begin
          response = execute(:post, table_name, {}, default_headers, generate_payload(table_name, entity))
          return parse_response(response)          
        rescue RestClient::RequestFailed          
          raise WAZ::Tables::EntityAlreadyExists, entity[:row_key] if $!.http_code == 409 and $!.response.body.include?('EntityAlreadyExists')          
        end     
      end
      
      # Update an existing entity on the current storage account.
      # TODO: handle specific errors
      def update_entity(table_name, entity)
        raise WAZ::Tables::InvalidTableName, table_name unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        response = execute(:put, "#{table_name}(PartitionKey='#{entity[:partition_key]}',RowKey='#{entity[:row_key]}')", {}, default_headers.merge({'If-Match' => '*'}) , generate_payload(table_name, entity))
        return parse_response(response)
      end
      
      # Merge an existing entity on the current storage account.
      # The Merge Entity operation updates an existing entity by updating the entity's properties. 
      # This operation does not replace the existing entity, as the Update Entity operation does
      # TODO: handle specific errors
      def merge_entity(table_name, entity)
        raise WAZ::Tables::InvalidTableName, table_name unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        response = execute(:merge, "#{table_name}(PartitionKey='#{entity[:partition_key]}',RowKey='#{entity[:row_key]}')", {}, default_headers.merge({'If-Match' => '*'}), generate_payload(table_name, entity))
        return parse_response(response)
      end
      
      # Delete an existing entity in a table.
      def delete_entity(table_name, partition_key, row_key)
        raise WAZ::Tables::InvalidTableName, table_name unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        
        begin
          execute :delete, "#{table_name}(PartitionKey='#{partition_key}',RowKey='#{row_key}')", {}, default_headers.merge({'If-Match' => '*'})
        rescue RestClient::ResourceNotFound
          raise WAZ::Tables::TableDoesNotExist, table_name if $!.http_code == 404 and $!.response.body.include?('TableNotFound')
          raise WAZ::Tables::EntityDoesNotExist, "(PartitionKey='#{partition_key}',RowKey='#{row_key}')" if $!.http_code == 404
        end
      end
      
      # Retrieves an existing entity on the current storage account.
      # TODO: handle specific errors
      def get_entity(table_name, partition_key, row_key)
        raise WAZ::Tables::InvalidTableName, table_name unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        response = execute(:get, "#{table_name}(PartitionKey='#{partition_key}',RowKey='#{row_key}')", {}, default_headers)
        return parse_response(response)
      end    
      
      # Retrieves a set of entities on the current storage account for a given query.
      # When the :top => n is passed it returns only the first n rows that match with the query
      # Optional parameters:
      # * :headers a hash containing the request headers
      # * :expression the filter query that will be executed against the table (see http://msdn.microsoft.com/en-us/library/dd179421.aspx for more information), 
      # * :top limits the amount of fields for this query. 
      # * :continuation_token the hash obtained when you perform a query that has more than 1000 records or exceeds the allowed timeout (see http://msdn.microsoft.com/en-us/library/dd135718.aspx)
      def query(table_name, options = {})
        raise WAZ::Tables::InvalidTableName, table_name unless WAZ::Storage::ValidationRules.valid_table_name?(table_name)
        query = {'$filter' => (options[:expression] or '') }
        query.merge!({ '$top' => options[:top] }) unless options[:top].nil?
        query.merge!(options[:continuation_token]) unless options[:continuation_token].nil?
        response = execute :get, "#{table_name}()", query, default_headers
        continuation_token = {'NextPartitionKey' => response.headers[:x_ms_continuation_nextpartitionkey], 'NextRowKey' => response.headers[:x_ms_continuation_nextrowkey]}
        parse_response(response, continuation_token)
      end

      private 
        def generate_payload(table_name, entity)
          payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>" \
                     "<entry xmlns:d=\"#{DATASERVICES_NAMESPACE}\" xmlns:m=\"#{DATASERVICES_METADATA_NAMESPACE}\" xmlns=\"http://www.w3.org/2005/Atom\">" \
                     "<id>#{generate_request_uri "#{table_name}"}(PartitionKey='#{entity[:partition_key]}',RowKey='#{entity[:row_key]}')</id>" \
                     "<title /><updated>#{Time.now.utc.iso8601}</updated><author><name /></author><link rel=\"edit\" title=\"#{table_name}\" href=\"#{table_name}(PartitionKey='#{entity[:partition_key]}',RowKey='#{entity[:row_key]}')\" />" \
                     "<content type=\"application/xml\"><m:properties>"

          entity.sort_by { |k| k.to_s }.each do |k,v| 
            value, type = EdmTypeHelper.parse_to(v)[0].to_s, EdmTypeHelper.parse_to(v)[1].to_s
            payload << (!v.nil? ? "<d:#{k.to_s} m:type=\"#{k.edm_type || type}\">#{value}</d:#{k.to_s}>" : "<d:#{k.to_s} m:type=\"#{k.edm_type || type}\" m:null=\"true\" />") unless k.eql?(:partition_key) or k.eql?(:row_key) 
          end  

          payload << "<d:PartitionKey>#{entity[:partition_key]}</d:PartitionKey>" \
                     "<d:RowKey>#{entity[:row_key]}</d:RowKey>" \
                     "</m:properties></content></entry>"
          return payload
        end
        
        def parse_response(response, continuation_token = nil)
          doc = REXML::Document.new(response)
          entities = REXML::XPath.each(doc, '//entry').map do |entry|
            fields = REXML::XPath.each(entry.elements['content'], 'm:properties/*', {"m" => DATASERVICES_METADATA_NAMESPACE}).map do |f|
              { f.name.gsub(/PartitionKey/i, 'partition_key').gsub(/RowKey/i, 'row_key').to_sym => EdmTypeHelper.parse_from(f) }
            end
            Hash[*fields.collect {|h| h.to_a}.flatten]
          end
          entities = WAZ::Tables::TableArray.new(entities)          
          entities.continuation_token = continuation_token
          return (REXML::XPath.first(doc, '/feed')) ? entities : entities.first
        end
        
        def default_headers
          { 'Date' => Time.new.httpdate, 
            'Content-Type' => 'application/atom+xml', 
            'DataServiceVersion' => '1.0;NetFx', 
            'MaxDataServiceVersion' => '1.0;NetFx' }
        end
    end
  end
end