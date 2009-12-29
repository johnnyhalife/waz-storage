module WAZ
  module Tables
    # This is internally used by the waz-tables part of the gem and it exposes the Windows Azure Blob API REST methods 
    # implementation. You can use this class to perform an specific operation that isn't provided by the current API.
    class Service
      include WAZ::Storage::SharedKeyCoreService

      # Creates a table on the current Windows Azure Storage account.
      def create_table(table_name)
        payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?><entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\"><title /><updated>#{Time.now.utc.iso8601}</updated><author><name/></author><id/><content type=\"application/xml\"><m:properties><d:TableName>#{table_name}</d:TableName></m:properties></content></entry>"
        
        begin
          execute :post, 'Tables', {}, { 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }, payload
        rescue RestClient::RequestFailed
          raise WAZ::Tables::TableAlreadyExists, table_name if $!.http_code == 409
        end
      end
      
      # Delete a table on the current Windows Azure Storage account.
      def delete_table(table_name)
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
      
      # Lists all existing tables on the current storage account.
      def get_table(table_name)
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
    end
  end
end