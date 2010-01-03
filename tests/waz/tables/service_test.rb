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
    expected_payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?><entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\"><title /><updated>#{Time.now.utc.iso8601}</updated><author><name/></author><id/><content type=\"application/xml\"><m:properties><d:TableName>Customers</d:TableName></m:properties></content></entry>"
    expected_headers = { 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }    
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute)
    service.expects(:generate_request_uri).with("Tables", {}).returns("http://localhost/Tables")
    service.expects(:generate_request).with(:post, "http://localhost/Tables", expected_headers, expected_payload).returns(RestClient::Request.new(:method => :post, :url => "http://localhost/Tables", :headers => expected_headers, :payload => expected_payload))
    service.create_table('Customers')
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
                       "<id>http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')</id>" \
                       "<title /><updated>#{Time.now.utc.iso8601}</updated><author><name /></author><link rel=\"edit\" title=\"Customers\" href=\"Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')\" />" \
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
                             "<d:PartitionKey>myPartitionKey</d:PartitionKey>" \
                             "<d:RowKey>myRowKey1</d:RowKey>" \
                             "<d:Timestamp m:type=\"Edm.DateTime\">#{Time.now.utc.iso8601}</d:Timestamp>" \
                       "</m:properties></content></entry>"
                       
    expected_headers = {'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}    

    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(expected_payload)
    service.expects(:generate_request_uri).with("Customers").returns("http://localhost/Customers")
    service.expects(:generate_request_uri).with("Customers", {}).returns("http://localhost/Customers")
    service.expects(:generate_request).with(:post, "http://localhost/Customers", expected_headers , expected_payload).returns(RestClient::Request.new(:method => :post, :url => "http://localhost/Customers", :headers => expected_headers, :payload => expected_payload))

    fields = {'Address' => { :type => 'String', :value => 'Mountain View'},
              'Age' => { :type => 'Int32', :value => 23},
              'AmountDue' => { :type => 'Double', :value => 200.23},
              'BinaryData' => { :type => 'Binary', :value => nil},
              'CustomerCode' => { :type => 'Guid', :value => 'c9da6455-213d-42c9-9a79-3e9149a57833'},
              'CustomerSince' =>{ :type => 'DateTime', :value => Time.now.utc.iso8601},
              'IsActive' =>{ :type => 'Boolean', :value => true},
              'NumOfOrders' => {:type => 'Int64', :value => 255}}
              
    entity = { :partition_key => 'myPartitionKey', :row_key => 'myRowKey1', :fields =>  fields }
    new_entity = service.insert_entity('Customers', entity)
    
    new_entity[:fields].length.should == 11
  end
  
  it "should update an existing entity" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    expected_payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>" \
                       "<entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\">" \
                       "<id>http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')</id>" \
                       "<title /><updated>#{Time.now.utc.iso8601}</updated><author><name /></author><link rel=\"edit\" title=\"Customers\" href=\"Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')\" />" \
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
                             "<d:PartitionKey>myPartitionKey</d:PartitionKey>" \
                             "<d:RowKey>myRowKey1</d:RowKey>" \
                             "<d:Timestamp m:type=\"Edm.DateTime\">#{Time.now.utc.iso8601}</d:Timestamp>" \
                       "</m:properties></content></entry>"                       
    expected_headers = {'If-Match' => '*', 'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}    
    
    RestClient::Request.any_instance.expects(:execute).returns(expected_payload)
    service.expects(:generate_request_uri).with("Customers").returns("http://localhost/Customers")
    service.expects(:generate_request_uri).with("Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')", {}).returns("http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')")
    service.expects(:generate_request).with(:put, "http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')", expected_headers , expected_payload).returns(RestClient::Request.new(:method => :post, :url => "http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')", :headers => expected_headers, :payload => expected_payload))

    fields = {'Address' => { :type => 'String', :value => 'Mountain View'},
              'Age' => { :type => 'Int32', :value => 23},
              'AmountDue' => { :type => 'Double', :value => 200.23},
              'BinaryData' => { :type => 'Binary', :value => nil},
              'CustomerCode' => { :type => 'Guid', :value => 'c9da6455-213d-42c9-9a79-3e9149a57833'},
              'CustomerSince' =>{ :type => 'DateTime', :value => Time.now.utc.iso8601},
              'IsActive' =>{ :type => 'Boolean', :value => true},
              'NumOfOrders' => {:type => 'Int64', :value => 255}}

    entity = { :partition_key => 'myPartitionKey', :row_key => 'myRowKey1', :fields =>  fields }
    updated_entity = service.update_entity('Customers', entity)
    updated_entity[:fields].length.should == 11
  end
  
  it "should throw TooManyProperties exception" do 
    fields = {}
    253.times { |i| fields.merge!({ "test#{i.to_s}" => { :type => 'String', :value => i.to_s}}) }
    entity = { :partition_key => 'myPartitionKey', :row_key => 'myRowKey1', :fields =>  fields }
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")       
    lambda {service.insert_entity('Customers', entity)}.should raise_error(WAZ::Tables::TooManyProperties, "The entity contains more properties than allowed (252). The entity has 253 properties.")        
  end

  it "should throw EntityAlreadyExists exception" do 
    fields = { 'name' => {  :type => 'String', :value => 'value'}}
    entity = { :partition_key => 'myPartitionKey', :row_key => 'myRowKey1', :fields =>  fields }

    response = mock()
    response.stubs(:body).returns('EntityAlreadyExists The specified entity already exists')        
    request_failed = RestClient::RequestFailed.new
    request_failed.stubs(:response).returns(response)    
    request_failed.stubs(:http_code).returns(409)

    RestClient::Request.any_instance.expects(:execute).raises(request_failed)    
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")       
    lambda {service.insert_entity('Customers', entity)}.should raise_error(WAZ::Tables::EntityAlreadyExists, "The specified entity already exists. RowKey: myRowKey1")
  end
  
  it "should delete an existing entity" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(mock())
    expected_headers = {'If-Match' => '*', 'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}
    service.expects(:generate_request_uri).with("Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')", {}).returns("http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1'")
    service.expects(:generate_request).with(:delete, "http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1'", expected_headers, nil).returns(RestClient::Request.new(:method => :post, :url => "http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1'", :headers => expected_headers))
    service.delete_entity('Customers', 'myPartitionKey', 'myRowKey1')
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
    lambda { service.delete_entity('table', 'myPartitionKey', 'myRowKey1') }.should raise_error(WAZ::Tables::EntityDoesNotExist, "The specified entity with (PartitionKey='myPartitionKey',RowKey='myRowKey1') does not exist.")
  end
  
  it "should get an entity by a given partitionkey and rowkey" do
    mock_response = <<-eom
    <?xml version="1.0" encoding="utf-8" standalone="yes"?>    
    <entry xml:base="http://myaccount.tables.core.windows.net/" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" m:etag="W/&quot;datetime'2010-01-01T15%3A50%3A49.9612116Z'&quot;" xmlns="http://www.w3.org/2005/Atom">
        <id>http://myaccount.tables.core.windows.net/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')</id>
        <title type="text"></title>
        <updated>2008-10-01T15:26:13Z</updated>
        <author>
          <name />
        </author>
        <link rel="edit" title="Customers" href="Customers (PartitionKey='myPartitionKey',RowKey='myRowKey1')" />
        <category term="myaccount.Customers" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
        <content type="application/xml">
          <m:properties>
            <d:PartitionKey>myPartitionKey</d:PartitionKey>
            <d:RowKey>myRowKey1</d:RowKey>
            <d:Timestamp m:type="Edm.DateTime">2008-10-01T15:26:04.6812774Z</d:Timestamp>
            <d:Address>123 Lakeview Blvd, Redmond WA 98052</d:Address>
            <d:CustomerSince m:type="Edm.DateTime">2008-10-01T15:25:05.2852025Z</d:CustomerSince>
            <d:Discount m:type="Edm.Double">10</d:Discount>
            <d:Rating16 m:type="Edm.Int16">3</d:Rating16>
            <d:Rating32 m:type="Edm.Int32">6</d:Rating32>
            <d:Rating64 m:type="Edm.Int64">9</d:Rating64>            
            <d:BinaryData m:type="Edm.Binary" m:null="true" />
            <d:SomeBoolean m:type="Edm.Boolean">true</d:SomeBoolean>
            <d:SomeSingle m:type="Edm.Single">9.3</d:SomeSingle>            
          </m:properties>
        </content>
      </entry>
    eom
    expected_headers = {'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}    

    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(mock_response)
    service.expects(:generate_request_uri).with("Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')", {}).returns("http://myaccount.tables.core.windows.net/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')")
    service.expects(:generate_request).with(:get, "http://myaccount.tables.core.windows.net/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')", expected_headers, nil).returns(RestClient::Request.new(:method => :post, :url => "http://myaccount.tables.core.windows.net(PartitionKey='myPartitionKey',RowKey='myRowKey1')", :headers => expected_headers))

    entity = service.get_entity('Customers', 'myPartitionKey', 'myRowKey1')

    entity[:table_name].should == 'Customers'
    entity[:partition_key].should == 'myPartitionKey'
    entity[:row_key].should == 'myRowKey1'
    entity[:etag].should == "W/\"datetime'2010-01-01T15%3A50%3A49.9612116Z'\""    
    entity[:fields].length.should == 12

    entity[:fields]['PartitionKey'].should  == { :type => 'String', :value => 'myPartitionKey'}    
    entity[:fields]['RowKey'].should        == { :type => 'String', :value => 'myRowKey1'}        
    entity[:fields]['Timestamp'].should     == { :type => 'DateTime', :value => '2008-10-01T15:26:04.6812774Z'}
    entity[:fields]['Address'].should       == { :type => 'String', :value => '123 Lakeview Blvd, Redmond WA 98052'}    
    entity[:fields]['CustomerSince'].should == { :type => 'DateTime', :value => '2008-10-01T15:25:05.2852025Z'}
    entity[:fields]['Discount'].should      == { :type => 'Double', :value => 10}
    entity[:fields]['Rating16'].should      == { :type => 'Int16', :value => 3}
    entity[:fields]['Rating32'].should      == { :type => 'Int32', :value => 6}
    entity[:fields]['Rating64'].should      == { :type => 'Int64', :value => 9}           
    entity[:fields]['BinaryData'].should    == { :type => 'Binary', :value => nil}            
    entity[:fields]['SomeBoolean'].should   == { :type => 'Boolean', :value => true}            
    entity[:fields]['SomeSingle'].should    == { :type => 'Single', :value => 9.3}                    
  end

  it "should get a set of entities" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "myaccount.tables.core.windows.net")    

    mock_response = <<-eom
    <?xml version="1.0" encoding="utf-8" standalone="yes"?>
    <feed xml:base="http://myaccount.tables.core.windows.net/" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns="http://www.w3.org/2005/Atom">
      <title type="text">Customers</title>
      <id>http://myaccount.tables.core.windows.net/Customers</id>
      <updated>2008-10-01T15:26:13Z</updated>
      <link rel="self" title="Customers" href="Customers" />    
      <entry xml:base="http://myaccount.tables.core.windows.net/" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" m:etag="W/&quot;datetime'2010-01-01T15%3A50%3A49.9612116Z'&quot;" xmlns="http://www.w3.org/2005/Atom">
        <id>http://myaccount.tables.core.windows.net/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')</id>
        <title type="text"></title>
        <updated>2008-10-01T15:26:13Z</updated>
        <author>
          <name />
        </author>
        <link rel="edit" title="Customers" href="Customers (PartitionKey='myPartitionKey',RowKey='myRowKey1')" />
        <category term="myaccount.Customers" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
        <content type="application/xml">
          <m:properties>
            <d:PartitionKey>myPartitionKey</d:PartitionKey>
            <d:RowKey>myRowKey1</d:RowKey>
            <d:Timestamp m:type="Edm.DateTime">2008-10-01T15:26:04.6812774Z</d:Timestamp>
            <d:Address>123 Lakeview Blvd, Redmond WA 98052</d:Address>
            <d:CustomerSince m:type="Edm.DateTime">2008-10-01T15:25:05.2852025Z</d:CustomerSince>
            <d:Discount m:type="Edm.Double">10</d:Discount>
            <d:Rating m:type="Edm.Int32">3</d:Rating>
            <d:BinaryData m:type="Edm.Binary" m:null="true" />
          </m:properties>
        </content>
      </entry>
      <entry xml:base="http://myaccount.tables.core.windows.net/" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" m:etag="W/&quot;datetime'2010-01-01T15%3A50%3A49.9612116Z'&quot;" xmlns="http://www.w3.org/2005/Atom">
        <id>http://myaccount.tables.core.windows.net/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey2')</id>
        <title type="text"></title>
        <updated>2008-10-01T15:26:13Z</updated>
        <author>
          <name />
        </author>
        <link rel="edit" title="Customers" href="Customers (PartitionKey='myPartitionKey',RowKey='myRowKey2')" />
        <category term="myaccount.Customers" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
        <content type="application/xml">
          <m:properties>
            <d:PartitionKey>myPartitionKey</d:PartitionKey>
            <d:RowKey>myRowKey2</d:RowKey>
            <d:Timestamp m:type="Edm.DateTime">2009-10-01T15:26:04.6812774Z</d:Timestamp>
            <d:Address>234 Lakeview Blvd, Redmond WA 98052</d:Address>
            <d:CustomerSince m:type="Edm.DateTime">2009-10-01T15:25:05.2852025Z</d:CustomerSince>
            <d:Discount m:type="Edm.Double">11</d:Discount>
            <d:Rating m:type="Edm.Int32">4</d:Rating>
            <d:BinaryData m:type="Edm.Binary">binary_data</d:BinaryData>
          </m:properties>
        </content>
      </entry>      
    </feed>  
    eom
    mock_response.stubs(:headers).returns({})            
    expected_headers = {'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}    
    expected_query = { '$filter' => "expression" }

    RestClient::Request.any_instance.expects(:execute).once().returns(mock_response)
    service.expects(:generate_request_uri).with("Customers()", expected_query).returns("http://myaccount.tables.core.windows.net/Customers()?$filter=expression")
    service.expects(:generate_request).with(:get, "http://myaccount.tables.core.windows.net/Customers()?$filter=expression", expected_headers, nil).returns(RestClient::Request.new(:method => :post, :url => "http://myaccount.tables.core.windows.net/Customers()?$filter=expression", :headers => expected_headers))
    entities = service.query_entity('Customers', 'expression')

    entities.length.should == 2
    entities[0][:table_name].should == 'Customers'
    entities[0][:partition_key].should == 'myPartitionKey'
    entities[0][:row_key].should == 'myRowKey1'
    entities[0][:fields].length.should == 8

    entities[0][:fields]['PartitionKey'].should   == { :type => 'String', :value => 'myPartitionKey'}    
    entities[0][:fields]['RowKey'].should         == { :type => 'String', :value => 'myRowKey1'}        
    entities[0][:fields]['Timestamp'].should      == { :type => 'DateTime', :value => '2008-10-01T15:26:04.6812774Z'}
    entities[0][:fields]['Address'].should        == { :type => 'String', :value => '123 Lakeview Blvd, Redmond WA 98052'}    
    entities[0][:fields]['CustomerSince'].should  == { :type => 'DateTime', :value => '2008-10-01T15:25:05.2852025Z'}
    entities[0][:fields]['Discount'].should       == { :type => 'Double', :value => 10}
    entities[0][:fields]['Rating'].should         == { :type => 'Int32', :value => 3}
    entities[0][:fields]['BinaryData'].should     == { :type => 'Binary', :value => nil}

    entities[1][:table_name].should == 'Customers'
    entities[1][:partition_key].should == 'myPartitionKey'
    entities[1][:row_key].should == 'myRowKey2'
    entities[1][:fields].length.should == 8

    entities[1][:fields]['PartitionKey'].should   == { :type => 'String', :value => 'myPartitionKey'}    
    entities[1][:fields]['RowKey'].should         == { :type => 'String', :value => 'myRowKey2'}        
    entities[1][:fields]['Timestamp'].should      == { :type => 'DateTime', :value => '2009-10-01T15:26:04.6812774Z'}
    entities[1][:fields]['Address'].should        == { :type => 'String', :value => '234 Lakeview Blvd, Redmond WA 98052'}    
    entities[1][:fields]['CustomerSince'].should  == { :type => 'DateTime', :value => '2009-10-01T15:25:05.2852025Z'}
    entities[1][:fields]['Discount'].should       == { :type => 'Double', :value => 11}
    entities[1][:fields]['Rating'].should         == { :type => 'Int32', :value => 4}
    entities[1][:fields]['BinaryData'].should     == { :type => 'Binary', :value => 'binary_data'}
  end
  
  it "should send the $top query parameter when calling the service with top option " do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "myaccount.tables.core.windows.net")    
    mock_response = <<-eom
    <?xml version="1.0" encoding="utf-8" standalone="yes"?>
    <feed xml:base="http://myaccount.tables.core.windows.net/" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns="http://www.w3.org/2005/Atom">
      <title type="text">Customers</title>
      <id>http://myaccount.tables.core.windows.net/Customers</id>
      <updated>2008-10-01T15:26:13Z</updated>
      <link rel="self" title="Customers" href="Customers" />    
      <entry xml:base="http://myaccount.tables.core.windows.net/" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" m:etag="W/&quot;datetime'2010-01-01T15%3A50%3A49.9612116Z'&quot;" xmlns="http://www.w3.org/2005/Atom">
        <id>http://myaccount.tables.core.windows.net/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')</id>
        <title type="text"></title>
        <updated>2008-10-01T15:26:13Z</updated>
        <author>
          <name />
        </author>
        <link rel="edit" title="Customers" href="Customers (PartitionKey='myPartitionKey',RowKey='myRowKey1')" />
        <category term="myaccount.Customers" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
        <content type="application/xml">
          <m:properties>
            <d:PartitionKey>myPartitionKey</d:PartitionKey>
            <d:RowKey>myRowKey1</d:RowKey>
            <d:Rating m:type="Edm.Int32">3</d:Rating>
          </m:properties>
        </content>
      </entry>
    </feed>  
    eom
    mock_response.stubs(:headers).returns({})        
    
    expected_headers = {'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}    
    expected_query = { '$filter' => "expression", '$top' => 1 }

    RestClient::Request.any_instance.expects(:execute).once().returns(mock_response)
    service.expects(:generate_request_uri).once().with("Customers()", expected_query).returns("http://myaccount.tables.core.windows.net/Customers()?$filter=expression&$top=1")
    service.expects(:generate_request).once().with(:get, "http://myaccount.tables.core.windows.net/Customers()?$filter=expression&$top=1", expected_headers, nil).returns(RestClient::Request.new(:method => :post, :url => "http://myaccount.tables.core.windows.net/Customers()?$filter=expression&$top=1", :headers => expected_headers))
    entities = service.query_entity('Customers', 'expression', 1)

    entities.length.should == 1
  end
  
  it "should call execute method recursively when there are continuation token headers" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "myaccount.tables.core.windows.net")
    sample_feed = <<-eom
    <?xml version="1.0" encoding="utf-8" standalone="yes"?>
    <feed xml:base="http://myaccount.tables.core.windows.net/" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns="http://www.w3.org/2005/Atom">
      <title type="text">Customers</title>
      <id>http://myaccount.tables.core.windows.net/Customers</id>
      <updated>2008-10-01T15:26:13Z</updated>
      <link rel="self" title="Customers" href="Customers" />    
      <entry xml:base="http://myaccount.tables.core.windows.net/" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" m:etag="W/&quot;datetime'2010-01-01T15%3A50%3A49.9612116Z'&quot;" xmlns="http://www.w3.org/2005/Atom">
        <id>http://myaccount.tables.core.windows.net/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')</id>
        <title type="text"></title>
        <updated>2008-10-01T15:26:13Z</updated>
        <author>
          <name />
        </author>
        <link rel="edit" title="Customers" href="Customers (PartitionKey='myPartitionKey',RowKey='myRowKey1')" />
        <category term="myaccount.Customers" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
        <content type="application/xml">
          <m:properties>
            <d:PartitionKey>myPartitionKey</d:PartitionKey>
            <d:RowKey>myRowKey1</d:RowKey>
            <d:Rating m:type="Edm.Int32">3</d:Rating>
          </m:properties>
        </content>
      </entry>
    </feed>  
    eom
    mock_response1, mock_response2 = sample_feed
    mock_response1.stubs(:headers).returns({:x_ms_continuation_nextpartitionkey => 'next_partition_key', :x_ms_continuation_nextrowkey => 'next_row_key'})    
    mock_response2.stubs(:headers).returns({})    
    
    expected_headers = {'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}    

    expected_query1 = { '$filter' => "expression" }    
    expected_query2 = { 'NextRowKey' => 'next_row_key', '$filter' => "expression", 'NextPartitionKey' => 'next_partition_key' }

    rest_client1 = RestClient::Request.new(:method => :post, :url => "http://myaccount.tables.core.windows.net/Customers()?$filter=expression", :headers => expected_headers)
    rest_client1.expects(:execute).once().returns(mock_response1)

    rest_client2 = RestClient::Request.new(:method => :post, :url => "http://myaccount.tables.core.windows.net/Customers()?$filter=expression&NextPartitionKey=next_partition_key&NextRowKey=next_row_key", :headers => expected_headers)
    rest_client2.expects(:execute).once().returns(mock_response2)

    service.expects(:generate_request_uri).with("Customers()", expected_query1).returns("http://myaccount.tables.core.windows.net/Customers()?$filter=expression")
    service.expects(:generate_request_uri).with("Customers()", expected_query2).returns("http://myaccount.tables.core.windows.net/Customers()?$filter=expression&NextPartitionKey=next_partition_key&NextRowKey=next_row_key")
    service.expects(:generate_request).once().with(:get, "http://myaccount.tables.core.windows.net/Customers()?$filter=expression", expected_headers, nil).returns(rest_client1)
    service.expects(:generate_request).once().with(:get, "http://myaccount.tables.core.windows.net/Customers()?$filter=expression&NextPartitionKey=next_partition_key&NextRowKey=next_row_key", expected_headers, nil).returns(rest_client2)
    entities = service.query_entity('Customers', 'expression')

   entities.length.should == 2
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
  
  it "should throw when invalid table name is provided" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")    
    lambda { service.get_entity('9existing', 'foo', 'foo') }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should throw when invalid table name is provided" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")    
    lambda { service.query_entity('9existing', 'foo') }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should throw when invalid table name is provided" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")    
    lambda { service.update_entity('9existing', {}) }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
end