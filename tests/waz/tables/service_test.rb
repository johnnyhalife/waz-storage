# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')

require 'rubygems'
require 'spec'
require 'mocha'
require 'restclient'
require 'time'
require 'hmac-sha2'
require 'base64'
require 'tests/configuration'
require 'lib/waz-tables'

describe "tables service behavior" do
  it "should get a table" do
    response = <<-eos
    <?xml version=\"1.0" encoding=\"utf-8" standalone=\"yes"?>
    <entry xml:base=\"http://wazstoragejohnny.table.core.windows.net/" xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns=\"http://www.w3.org/2005/Atom">
      <id>http://myaccount.table.core.windows.net/Tables('table1')</id>
      <title type=\"text"></title>
      <updated>2009-12-28T02:00:21Z</updated>
      <author>
        <name />
      </author>
      <link rel=\"edit" title=\"Tables" href=\"Tables('table1')" />
      <category term=\"wazstoragejohnny.Tables" scheme=\"http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
      <content type=\"application/xml">
        <m:properties>
          <d:TableName>table1</d:TableName>
        </m:properties>
      </content>
    </entry>
    eos
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    response.stubs(:headers).returns({:x_ms_continuation_nexttablename => 'next-table'})

    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with("Tables('table1')", {}, nil).returns("http://localhost/Tables('table1')")
    service.expects(:generate_request).with(:get, "http://localhost/Tables('table1')", { 'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx'}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost/Tables('table1')"))
    table = service.get_table('table1')
    table[:name].should == 'table1'
    table[:url].should == "http://myaccount.table.core.windows.net/Tables('table1')"    
  end  
  
  it "should list all tables" do
    response = <<-eos
    <?xml version=\"1.0" encoding=\"utf-8" standalone=\"yes"?>
    <feed xml:base=\"http://myaccount.tables.core.windows.net/" xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns=\"http://www.w3.org/2005/Atom">
      <title type=\"text">Tables</title>
      <id>http://myaccount.tables.core.windows.net/Tables</id>
      <updated>2009-01-04T17:18:54.7062347Z</updated>
      <link rel=\"self" title=\"Tables" href=\"Tables" />
      <entry>
        <id>http://myaccount.tables.core.windows.net/Tables('table1')</id>
        <title type=\"text"></title>
        <updated>2009-01-04T17:18:54.7062347Z</updated>
        <author>
          <name />
        </author>
        <link rel=\"edit" title=\"Tables" href=\"Tables('table1')" />
        <category term=\"myaccount.Tables" scheme=\"http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
        <content type=\"application/xml">
          <m:properties>
            <d:TableName>table1</d:TableName>
          </m:properties>
        </content>
      </entry>
      <entry>
        <id>http://myaccount.tables.core.windows.net/Tables('table2')</id>
        <title type=\"text"></title>
        <updated>2009-01-04T17:18:54.7062347Z</updated>
        <author>
          <name />
        </author>
        <link rel=\"edit" title=\"Tables" href=\"Tables('table2')" />
        <category term=\"myaccount.Tables" scheme=\"http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
        <content type=\"application/xml">
          <m:properties>
            <d:TableName>table2</d:TableName>
          </m:properties>
        </content>
      </entry>
    </feed>
    eos
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    response.stubs(:headers).returns({:x_ms_continuation_nexttablename => 'next-table'})

    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with("Tables", {}, nil).returns("http://localhost/Tables")
    service.expects(:generate_request).with(:get, "http://localhost/Tables", { 'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx'}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost/Tables"))
    tables, next_table_name = service.list_tables
    tables.length.should == 2
    tables.first()[:name].should == 'table1'
    tables.first()[:url].should == "http://myaccount.tables.core.windows.net/Tables('table1')"    
    tables.last()[:name].should == 'table2'
    tables.last()[:url].should == "http://myaccount.tables.core.windows.net/Tables('table2')"
    next_table_name.should == 'next-table'        
  end
  
  it "should include the NextTableName parameter when a continuation token is provided" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    response = ''
    response.stubs(:headers).returns({:x_ms_continuation_nexttablename => 'next-table'})
    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with("Tables", { 'NextTableName' => 'next-table-name' }, nil).returns("http://localhost/Tables?NextTableName=next-table-name")
    service.expects(:generate_request).with(:get, "http://localhost/Tables?NextTableName=next-table-name", {'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx'}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost/Tables?NextTableName=next-table-name"))
    tables, next_table_name = service.list_tables('next-table-name')
  end
  
  it "should create a table" do
    expected_payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?><entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\"><title /><updated>#{Time.now.utc.iso8601}</updated><author><name/></author><id/><content type=\"application/xml\"><m:properties><d:TableName>mocktable</d:TableName></m:properties></content></entry>"
    expected_headers = { 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }    
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute)
    service.expects(:generate_request_uri).with("Tables", {}).returns("http://localhost/Tables")
    service.expects(:generate_request).with(:post, "http://localhost/Tables", expected_headers, expected_payload).returns(RestClient::Request.new(:method => :post, :url => "http://localhost/Tables", :headers => expected_headers, :payload => expected_payload))
    service.create_table('mocktable')
  end
    
  it "should throw when a table already exists" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    failed_response = RestClient::RequestFailed.new
    failed_response.stubs(:http_code).returns(409)    
    RestClient::Request.any_instance.expects(:execute).raises(failed_response)
    lambda { service.create_table('existingtable') }.should raise_error(WAZ::Tables::TableAlreadyExists, "The table existingtable already exists on your account.")
  end
  
  it "should delete a table" do
    expected_headers = { 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    
    response = mock()
    response.stubs(:code).returns(204)
    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with("Tables('tabletodelete')", {}).returns("http://localhost/Tables('tabletodelete')")
    service.expects(:generate_request).with(:delete, "http://localhost/Tables('tabletodelete')", expected_headers, nil).returns(RestClient::Request.new(:method => :delete, :url => "http://localhost/Tables('tabletodelete')", :headers => expected_headers))
    service.delete_table('tabletodelete')
  end
  
  it "should throw when trying to get an unexisting table" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    failed_response = RestClient::ResourceNotFound.new
    failed_response.stubs(:http_code).returns(404)    
    RestClient::Request.any_instance.expects(:execute).raises(failed_response)
    lambda { service.get_table('unexistingtable') }.should raise_error(WAZ::Tables::TableDoesNotExist, "The specified table unexistingtable does not exist.")
  end
    
  it "should throw when trying to delete an unexisting table" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    failed_response = RestClient::ResourceNotFound.new
    failed_response.stubs(:http_code).returns(404)    
    RestClient::Request.any_instance.expects(:execute).raises(failed_response)
    lambda { service.delete_table('unexistingtable') }.should raise_error(WAZ::Tables::TableDoesNotExist, "The specified table unexistingtable does not exist.")
  end
  
  it "should insert a new entity" do
    expected_payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>" \
                       "<entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\">" \
                       "<title /><updated>#{Time.now.utc.iso8601}</updated><author><name /></author><id />" \
                       "<content type=\"application/xml\">" \
                       "<m:properties>" \
                             "<d:Address m:type=\"Edm.String\">Mountain View</d:Address>" \
                             "<d:Age m:type=\"Edm.Int32\">23</d:Age>" \
                             "<d:AmountDue m:type=\"Edm.Double\">200.23</d:AmountDue>" \
                             "<d:BinaryData m:type=\"Edm.Binary\" m:null=\"true\" />" \
                             "<d:CustomerCode m:type=\"Edm.Guid\">c9da6455-213d-42c9-9a79-3e9149a57833</d:CustomerCode>" \
                             "<d:CustomerSince m:type=\"Edm.DateTime\">#{Time.now.utc.iso8601}</d:CustomerSince>" \
                             "<d:IsActive m:type=\"Edm.Boolean\">true</d:IsActive>" \
                             "<d:NumOfOrders m:type=\"Edm.Int64\">255</d:NumOfOrders>" \
                             "<d:PartitionKey>mypartitionkey</d:PartitionKey>" \
                             "<d:RowKey>myrowkey1</d:RowKey>" \
                             "<d:Timestamp m:type=\"Edm.DateTime\">#{Time.now.utc.iso8601}</d:Timestamp>" \
                       "</m:properties></content></entry>"
                       
    expected_headers = {'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}    

    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(mock())
    service.expects(:generate_request_uri).with("mocktable", {}).returns("http://localhost/mocktable")
    service.expects(:generate_request).with(:post, "http://localhost/mocktable", expected_headers , expected_payload).returns(RestClient::Request.new(:method => :post, :url => "http://localhost/mocktable", :headers => expected_headers, :payload => expected_payload))

    fields = []
    fields << { :name => 'Address', :type => 'String', :value => 'Mountain View'}
    fields << { :name => 'Age', :type => 'Int32', :value => 23}
    fields << { :name => 'AmountDue', :type => 'Double', :value => 200.23}
    fields << { :name => 'BinaryData', :type => 'Binary', :value => nil}
    fields << { :name => 'CustomerCode', :type => 'Guid', :value => 'c9da6455-213d-42c9-9a79-3e9149a57833'}            
    fields << { :name => 'CustomerSince', :type => 'DateTime', :value => Time.now.utc.iso8601}
    fields << { :name => 'IsActive', :type => 'Boolean', :value => true}
    fields << { :name => 'NumOfOrders', :type => 'Int64', :value => 255}    

    entity = { :partition_key => 'mypartitionkey', :row_key => 'myrowkey1', :fields =>  fields }

    service.insert_entity('mocktable', entity)
  end
  
  it "should throw TooManyProperties exception" do 
    fields = []
    253.times { |i| fields << { :name => 'test' + i.to_s, :type => 'String', :value => i.to_s} }
    entity = { :partition_key => 'mypartitionkey', :row_key => 'myrowkey1', :fields =>  fields }
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")       
    lambda {service.insert_entity('mocktable', entity)}.should raise_error(WAZ::Tables::TooManyProperties, "The entity contains more properties than allowed (252). The entity has 253 properties.")        
  end

  it "should throw EntityAlreadyExists exception" do 
    fields = [{ :name => 'name', :type => 'String', :value => 'value'} ]
    entity = { :partition_key => 'mypartitionkey', :row_key => 'myrowkey1', :fields =>  fields }

    response = mock()
    response.stubs(:body).returns('EntityAlreadyExists The specified entity already exists')        
    request_failed = RestClient::RequestFailed.new
    request_failed.stubs(:response).returns(response)    
    request_failed.stubs(:http_code).returns(409)

    RestClient::Request.any_instance.expects(:execute).raises(request_failed)    
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")       
    lambda {service.insert_entity('mocktable', entity)}.should raise_error(WAZ::Tables::EntityAlreadyExists, "The specified entity already exists. RowKey: myrowkey1")
  end
  
  it "should delete an existing entity" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(mock())
    expected_headers = {'If-Match' => '*', 'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}
    service.expects(:generate_request_uri).with("mocktable(PartitionKey='myPartitionKey',RowKey='myRowKey1')", {}).returns("http://localhost/mocktable(PartitionKey='myPartitionKey',RowKey='myRowKey1'")
    service.expects(:generate_request).with(:delete, "http://localhost/mocktable(PartitionKey='myPartitionKey',RowKey='myRowKey1'", expected_headers, nil).returns(RestClient::Request.new(:method => :post, :url => "http://localhost/mocktable(PartitionKey='myPartitionKey',RowKey='myRowKey1'", :headers => expected_headers))
    service.delete_entity('mocktable', 'myPartitionKey', 'myRowKey1')
  end
  
  it "should throw when trying to delete and entity and the table does not exists" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")

    response = mock()
    response.stubs(:body).returns('TableNotFound')        
    request_failed = RestClient::ResourceNotFound.new
    request_failed.stubs(:response).returns(response)    
    request_failed.stubs(:http_code).returns(404)

    RestClient::Request.any_instance.expects(:execute).raises(request_failed)
    lambda { service.delete_entity('unexistingtable', 'myPartitionKey', 'myRowKey1') }.should raise_error(WAZ::Tables::TableDoesNotExist, "The specified table unexistingtable does not exist.")
  end
  
  it "should throw when trying to delete and entity and the table does not exists" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")

    response = mock()
    response.stubs(:body).returns('ResourceNotFound')        
    request_failed = RestClient::ResourceNotFound.new
    request_failed.stubs(:response).returns(response)    
    request_failed.stubs(:http_code).returns(404)

    RestClient::Request.any_instance.expects(:execute).raises(request_failed)
    lambda { service.delete_entity('existing', 'myPartitionKey', 'myRowKey1') }.should raise_error(WAZ::Tables::EntityDoesNotExist, "The specified entity with (PartitionKey='myPartitionKey',RowKey='myRowKey1') does not exist.")
  end
  
  it "should throw when invalid table name is provided" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")    
    lambda { service.create_table('9existing') }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should throw when invalid table name is provided" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")    
    lambda { service.delete_table('9existing') }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end

  it "should throw when invalid table name is provided" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")    
    lambda { service.get_table('9existing') }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end

  it "should throw when invalid table name is provided" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")    
    lambda { service.insert_entity('9existing', 'entity') }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should throw when invalid table name is provided" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")    
    lambda { service.delete_entity('9existing', 'foo', 'foo') }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
end