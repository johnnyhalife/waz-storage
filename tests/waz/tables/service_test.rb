# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')
require 'tests/configuration'
require 'lib/waz-tables'

describe "tables service behavior" do
  
  before do
    @table_service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
  end
  
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
    
    response.stubs(:headers).returns({:x_ms_continuation_nexttablename => 'next-table'})

    RestClient::Request.any_instance.expects(:execute).returns(response)
    @table_service.expects(:generate_request_uri).with("Tables('table1')", {}, nil).returns("http://localhost/Tables('table1')")
    @table_service.expects(:generate_request).with(:get, "http://localhost/Tables('table1')", { 'Content-Type' => 'application/atom+xml', 'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx'}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost/Tables('table1')"))
    table = @table_service.get_table('table1')
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
    response.stubs(:headers).returns({:x_ms_continuation_nexttablename => 'next-table'})

    RestClient::Request.any_instance.expects(:execute).returns(response)
    @table_service.expects(:generate_request_uri).with("Tables", {}, nil).returns("http://localhost/Tables")
    @table_service.expects(:generate_request).with(:get, "http://localhost/Tables", {'Content-Type' => 'application/atom+xml', 'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx'}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost/Tables"))
    tables, next_table_name = @table_service.list_tables
    tables.length.should == 2
    tables.first()[:name].should == 'table1'
    tables.first()[:url].should == "http://myaccount.tables.core.windows.net/Tables('table1')"    
    tables.last()[:name].should == 'table2'
    tables.last()[:url].should == "http://myaccount.tables.core.windows.net/Tables('table2')"
    next_table_name.should == 'next-table'        
  end
  
  it "should include the NextTableName parameter when a continuation token is provided" do
    response = ''
    response.stubs(:headers).returns({:x_ms_continuation_nexttablename => 'next-table'})
    RestClient::Request.any_instance.expects(:execute).returns(response)
    @table_service.expects(:generate_request_uri).with("Tables", { 'NextTableName' => 'next-table-name' }, nil).returns("http://localhost/Tables?NextTableName=next-table-name")
    @table_service.expects(:generate_request).with(:get, "http://localhost/Tables?NextTableName=next-table-name", {'Content-Type' => 'application/atom+xml','Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx'}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost/Tables?NextTableName=next-table-name"))
    tables, next_table_name = @table_service.list_tables('next-table-name')
  end
  
  it "should create a table" do
    expected_payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?><entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\"><title /><updated>#{Time.now.utc.iso8601}</updated><author><name/></author><id/><content type=\"application/xml\"><m:properties><d:TableName>Customers</d:TableName></m:properties></content></entry>"
    expected_headers = { 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }    
    RestClient::Request.any_instance.expects(:execute)
    @table_service.expects(:generate_request_uri).with("Tables", {}).returns("http://localhost/Tables")
    @table_service.expects(:generate_request).with(:post, "http://localhost/Tables", expected_headers, expected_payload).returns(RestClient::Request.new(:method => :post, :url => "http://localhost/Tables", :headers => expected_headers, :payload => expected_payload))
    @table_service.create_table('Customers')
  end
    
  it "should throw when a table already exists" do
    failed_response = RestClient::RequestFailed.new
    failed_response.stubs(:http_code).returns(409)    
    RestClient::Request.any_instance.expects(:execute).raises(failed_response)
    lambda { @table_service.create_table('existingtable') }.should raise_error(WAZ::Tables::TableAlreadyExists, "The table existingtable already exists on your account.")
  end
  
  it "should delete a table" do
    expected_headers = { 'Date' => Time.new.httpdate, 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }
    
    response = mock()
    response.stubs(:code).returns(204)
    RestClient::Request.any_instance.expects(:execute).returns(response)
    @table_service.expects(:generate_request_uri).with("Tables('tabletodelete')", {}).returns("http://localhost/Tables('tabletodelete')")
    @table_service.expects(:generate_request).with(:delete, "http://localhost/Tables('tabletodelete')", expected_headers, nil).returns(RestClient::Request.new(:method => :delete, :url => "http://localhost/Tables('tabletodelete')", :headers => expected_headers))
    @table_service.delete_table('tabletodelete')
  end
  
  it "should throw when trying to get an unexisting table" do
    failed_response = RestClient::ResourceNotFound.new
    failed_response.stubs(:http_code).returns(404)    
    RestClient::Request.any_instance.expects(:execute).raises(failed_response)
    lambda { @table_service.get_table('unexistingtable') }.should raise_error(WAZ::Tables::TableDoesNotExist, "The specified table unexistingtable does not exist.")
  end
    
  it "should throw when trying to delete an unexisting table" do
    failed_response = RestClient::ResourceNotFound.new
    failed_response.stubs(:http_code).returns(404)    
    RestClient::Request.any_instance.expects(:execute).raises(failed_response)
    lambda { @table_service.delete_table('unexistingtable') }.should raise_error(WAZ::Tables::TableDoesNotExist, "The specified table unexistingtable does not exist.")
  end
  
  it "should insert a new entity" do
    expected_payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>" \
                       "<entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\">" \
                       "<id>http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')</id>" \
                       "<title /><updated>#{Time.now.utc.iso8601}</updated><author><name /></author><link rel=\"edit\" title=\"Customers\" href=\"Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')\" />" \
                       "<content type=\"application/xml\">" \
                       "<m:properties>" \
                             "<d:address m:type=\"Edm.String\">Mountain View</d:address>" \
                             "<d:age m:type=\"Edm.Int32\">23</d:age>" \
                             "<d:amount_due m:type=\"Edm.Double\">200.23</d:amount_due>" \
                             "<d:binary_data m:type=\"Edm.Binary\" m:null=\"true\" />" \
                             "<d:customer_code m:type=\"Edm.Guid\">c9da6455-213d-42c9-9a79-3e9149a57833</d:customer_code>" \
                             "<d:customer_since m:type=\"Edm.DateTime\">#{Time.now.utc.iso8601}</d:customer_since>" \
                             "<d:is_active m:type=\"Edm.Boolean\">true</d:is_active>" \
                             "<d:num_of_orders m:type=\"Edm.Int64\">255</d:num_of_orders>" \
                             "<d:PartitionKey>myPartitionKey</d:PartitionKey>" \
                             "<d:RowKey>myRowKey1</d:RowKey>" \
                       "</m:properties></content></entry>"
                       
    expected_headers = {'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}    

    RestClient::Request.any_instance.expects(:execute).returns(expected_payload)
    @table_service.expects(:generate_request_uri).with("Customers").returns("http://localhost/Customers")
    @table_service.expects(:generate_request_uri).with("Customers", {}).returns("http://localhost/Customers")
    request = RestClient::Request.new(:method => :post, :url => "http://localhost/Customers", :headers => expected_headers, :payload => expected_payload)
    @table_service.expects(:generate_request).with(:post, "http://localhost/Customers", expected_headers , expected_payload).returns(request)

    :binary_data.edm_type = 'Edm.Binary'
    :customer_code.edm_type = 'Edm.Guid'
    :num_of_orders.edm_type = 'Edm.Int64'
            
    entity = { :address => 'Mountain View',
               :age => 23,
               :amount_due => 200.23,
               :binary_data => nil,
               :customer_code => 'c9da6455-213d-42c9-9a79-3e9149a57833',
               :customer_since => Time.now.utc,
               :is_active => true,
               :num_of_orders => 255,
               :partition_key => 'myPartitionKey',
               :row_key => 'myRowKey1' }
               
    new_entity = @table_service.insert_entity('Customers', entity)
    new_entity.length.should == 10
    new_entity.reject{|k,v| entity.keys.include?(k) and entity.values.to_s.include?(v.to_s)}.length.should == 0
  end
  
  it "should update an existing entity" do
    expected_payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>" \
                       "<entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\">" \
                       "<id>http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')</id>" \
                       "<title /><updated>#{Time.now.utc.iso8601}</updated><author><name /></author><link rel=\"edit\" title=\"Customers\" href=\"Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')\" />" \
                       "<content type=\"application/xml\">" \
                       "<m:properties>" \
                           "<d:Timestamp m:type=\"Edm.DateTime\">#{Time.now.utc.iso8601}</d:Timestamp>" \
                           "<d:address m:type=\"Edm.String\">Mountain View</d:address>" \
                           "<d:age m:type=\"Edm.Int32\">23</d:age>" \
                           "<d:amount_due m:type=\"Edm.Double\">200.23</d:amount_due>" \
                           "<d:binary_data m:type=\"Edm.Binary\">#{Base64.encode64(File.read(__FILE__))}</d:binary_data>" \
                           "<d:customer_code m:type=\"Edm.Guid\">c9da6455-213d-42c9-9a79-3e9149a57833</d:customer_code>" \
                           "<d:customer_since m:type=\"Edm.DateTime\">#{Time.now.utc.iso8601}</d:customer_since>" \
                           "<d:is_active m:type=\"Edm.Boolean\">true</d:is_active>" \
                           "<d:num_of_orders m:type=\"Edm.Int64\">255</d:num_of_orders>" \
                           "<d:PartitionKey>myPartitionKey</d:PartitionKey>" \
                           "<d:RowKey>myRowKey1</d:RowKey>" \
                       "</m:properties></content></entry>"                       
    expected_headers = {'If-Match' => '*', 'Content-Type' => 'application/atom+xml', 'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx'}    
    expected_url = "http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')"

    RestClient::Request.any_instance.expects(:execute).returns(expected_payload)
    @table_service.expects(:generate_request_uri).with("Customers").returns("http://localhost/Customers")
    @table_service.expects(:generate_request_uri).with("Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')", {}).returns(expected_url)
    request = RestClient::Request.new(:method => :put, :url => expected_url, :headers => expected_headers, :payload => expected_payload)
    @table_service.expects(:generate_request).with(:put, expected_url, expected_headers , expected_payload).returns(request)

    :binary_data.edm_type = 'Edm.Binary'
    :customer_code.edm_type = 'Edm.Guid'
    :num_of_orders.edm_type = 'Edm.Int64'
    
    entity = { :address => 'Mountain View',
               :age => 23,
               :amount_due => 200.23,
               :binary_data => File.open(__FILE__),
               :customer_code => 'c9da6455-213d-42c9-9a79-3e9149a57833',
               :customer_since => Time.now.utc,
               :is_active => true,
               :num_of_orders => 255,
               :partition_key => 'myPartitionKey',
               :row_key => 'myRowKey1',
               :Timestamp => Time.now.utc}


    updated_entity = @table_service.update_entity('Customers', entity)

    updated_entity.length.should == 11
    # 1 because of binary_data
    updated_entity.reject{|k,v| entity.keys.include?(k) and entity.values.to_s.include?(v.to_s)}.length.should == 1
    entity[:binary_data].pos = 0
    updated_entity[:binary_data].read.should == entity[:binary_data].read
  end

  it "should merge an existing entity" do
    expected_payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>" \
                       "<entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\">" \
                       "<id>http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')</id>" \
                       "<title /><updated>#{Time.now.utc.iso8601}</updated><author><name /></author><link rel=\"edit\" title=\"Customers\" href=\"Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')\" />" \
                       "<content type=\"application/xml\">" \
                       "<m:properties>" \
                         "<d:Timestamp m:type=\"Edm.DateTime\">#{Time.now.utc.iso8601}</d:Timestamp>" \
                         "<d:address m:type=\"Edm.String\">Mountain View</d:address>" \
                         "<d:age m:type=\"Edm.Int32\">23</d:age>" \
                         "<d:amount_due m:type=\"Edm.Double\">200.23</d:amount_due>" \
                         "<d:binary_data m:type=\"Edm.Binary\" m:null=\"true\" />" \
                         "<d:customer_code m:type=\"Edm.Guid\">c9da6455-213d-42c9-9a79-3e9149a57833</d:customer_code>" \
                         "<d:customer_since m:type=\"Edm.DateTime\">#{Time.now.utc.iso8601}</d:customer_since>" \
                         "<d:is_active m:type=\"Edm.Boolean\">true</d:is_active>" \
                         "<d:num_of_orders m:type=\"Edm.Int64\">255</d:num_of_orders>" \
                         "<d:PartitionKey>myPartitionKey</d:PartitionKey>" \
                         "<d:RowKey>myRowKey1</d:RowKey>" \
                       "</m:properties></content></entry>"                       
                       
    expected_headers = {'If-Match' => '*', 'Content-Type' => 'application/atom+xml', 'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx'}        
    expected_url = "http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')"
    request = RestClient::Request.new(:method => :merge, :url => expected_url, :headers => expected_headers, :payload => expected_payload)    

    RestClient::Request.any_instance.expects(:execute).returns(expected_payload)
    @table_service.expects(:generate_request_uri).with("Customers").returns("http://localhost/Customers")
    @table_service.expects(:generate_request_uri).with("Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')", {}).returns(expected_url)
    @table_service.expects(:generate_request).with(:merge, expected_url, expected_headers , expected_payload).returns(request)

    :binary_data.edm_type = 'Edm.Binary'
    :customer_code.edm_type = 'Edm.Guid'
    :num_of_orders.edm_type = 'Edm.Int64'

    entity = { :address => 'Mountain View',
               :age => 23,
               :amount_due => 200.23,
               :binary_data => nil,
               :customer_code => 'c9da6455-213d-42c9-9a79-3e9149a57833',
               :customer_since => Time.now.utc,
               :is_active => true,
               :num_of_orders => 255,
               :partition_key => 'myPartitionKey',
               :row_key => 'myRowKey1',
               :Timestamp => Time.now.utc}

    merged_entity = @table_service.merge_entity('Customers', entity)
    merged_entity.length.should == 11
    merged_entity.reject{|k,v| entity.keys.include?(k) and entity.values.to_s.include?(v.to_s)}.length.should == 0
  end
  
  it "should throw TooManyProperties exception" do 
    long_entity = {}
    253.times { |i| long_entity.merge!({ "test#{i.to_s}".to_sym => "value#{i}"} ) }       
    lambda {@table_service.insert_entity('Customers', long_entity)}.should raise_error(WAZ::Tables::TooManyProperties, "The entity contains more properties than allowed (252). The entity has 253 properties.")        
  end

  it "should throw EntityAlreadyExists exception" do 

    entity = { :partition_key => 'myPartitionKey', :row_key => 'myRowKey1', :name => 'name' }

    response = mock()
    response.stubs(:body).returns('EntityAlreadyExists The specified entity already exists')        
    request_failed = RestClient::RequestFailed.new
    request_failed.stubs(:response).returns(response)    
    request_failed.stubs(:http_code).returns(409)

    RestClient::Request.any_instance.expects(:execute).raises(request_failed)    
    lambda {@table_service.insert_entity('Customers', entity)}.should raise_error(WAZ::Tables::EntityAlreadyExists, "The specified entity already exists. RowKey: myRowKey1")
  end
  
  it "should delete an existing entity" do
    RestClient::Request.any_instance.expects(:execute).returns(mock())
    expected_headers = {'If-Match' => '*', 'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}
    expected_url = "http://localhost/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1'"
    request = RestClient::Request.new(:method => :post, :url => expected_url, :headers => expected_headers)
    @table_service.expects(:generate_request_uri).with("Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')", {}).returns(expected_url)
    @table_service.expects(:generate_request).with(:delete, expected_url, expected_headers, nil).returns(request)
    @table_service.delete_entity('Customers', 'myPartitionKey', 'myRowKey1')
  end
  
  it "should throw when trying to delete and entity and the table does not exists" do
    response = mock()
    response.stubs(:body).returns('TableNotFound')        
    request_failed = RestClient::ResourceNotFound.new
    request_failed.stubs(:response).returns(response)    
    request_failed.stubs(:http_code).returns(404)

    RestClient::Request.any_instance.expects(:execute).raises(request_failed)
    lambda { @table_service.delete_entity('unexistingtable', 'myPartitionKey', 'myRowKey1') }.should raise_error(WAZ::Tables::TableDoesNotExist, "The specified table unexistingtable does not exist.")
  end
  
  it "should throw when trying to delete and entity and the table does not exists" do
    response = mock()
    response.stubs(:body).returns('ResourceNotFound')        
    request_failed = RestClient::ResourceNotFound.new
    request_failed.stubs(:response).returns(response)    
    request_failed.stubs(:http_code).returns(404)

    RestClient::Request.any_instance.expects(:execute).raises(request_failed)
    lambda { @table_service.delete_entity('table', 'myPartitionKey', 'myRowKey1') }.should raise_error(WAZ::Tables::EntityDoesNotExist, "The specified entity with (PartitionKey='myPartitionKey',RowKey='myRowKey1') does not exist.")
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
    expected_url = "http://myaccount.tables.core.windows.net/Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')"
    RestClient::Request.any_instance.expects(:execute).returns(mock_response)
    @table_service.expects(:generate_request_uri).with("Customers(PartitionKey='myPartitionKey',RowKey='myRowKey1')", {}).returns(expected_url)
    @table_service.expects(:generate_request).with(:get, expected_url, expected_headers, nil).returns(RestClient::Request.new(:method => :post, :url => expected_url, :headers => expected_headers))

    entity = @table_service.get_entity('Customers', 'myPartitionKey', 'myRowKey1')

    entity.length.should == 12
    
    entity[:partition_key].should == 'myPartitionKey'
    entity[:row_key].should == 'myRowKey1'
    entity[:Timestamp].should == Time.parse('2008-10-01T15:26:04.6812774Z')
    entity[:Address].should == '123 Lakeview Blvd, Redmond WA 98052'
    entity[:CustomerSince].should == Time.parse('2008-10-01T15:25:05.2852025Z')
    entity[:Discount].should == 10
    entity[:Rating16].should ==  3
    entity[:Rating32].should == 6
    entity[:Rating64].should == 9
    entity[:BinaryData].should == nil
    entity[:SomeBoolean].should ==  true
    entity[:SomeSingle].should == 9.3
  end

  it "should get a set of entities" do
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
            <d:BinaryData m:type="Edm.Binary" m:null="true" />            
          </m:properties>
        </content>
      </entry>      
    </feed>  
    eom
    mock_response.stubs(:headers).returns({})            
    expected_headers = {'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}    
    expected_url = "http://myaccount.tables.core.windows.net/Customers()?$filter=expression"
    expected_query = { '$filter' => "expression" }

    RestClient::Request.any_instance.expects(:execute).once().returns(mock_response)
    @table_service.expects(:generate_request_uri).with("Customers()", expected_query).returns(expected_url)
    @table_service.expects(:generate_request).with(:get, expected_url, expected_headers, nil).returns(RestClient::Request.new(:method => :post, :url => expected_url, :headers => expected_headers))
    entities = @table_service.query('Customers', {:expression => 'expression'})

    entities.length.should == 2
    entities.continuation_token[:next_partition_key].nil?.should == true
    entities.continuation_token[:next_row_key].nil?.should == true    

    entities.first.length.should == 8
    entities.first[:partition_key].should == 'myPartitionKey'
    entities.first[:row_key].should == 'myRowKey1'
    entities.first[:Timestamp].should == Time.parse('2008-10-01T15:26:04.6812774Z')
    entities.first[:Address].should == '123 Lakeview Blvd, Redmond WA 98052'
    entities.first[:CustomerSince].should == Time.parse('2008-10-01T15:25:05.2852025Z')
    entities.first[:Discount].should == 10
    entities.first[:Rating].should ==  3
    entities.first[:BinaryData].should == nil

    entities.last.length.should == 8
    entities.last[:partition_key].should == 'myPartitionKey'
    entities.last[:row_key].should == 'myRowKey2'
    entities.last[:Timestamp].should == Time.parse('2009-10-01T15:26:04.6812774Z')
    entities.last[:Address].should == '234 Lakeview Blvd, Redmond WA 98052'
    entities.last[:CustomerSince].should == Time.parse('2009-10-01T15:25:05.2852025Z')
    entities.last[:Discount].should == 11
    entities.last[:Rating].should ==  4
    entities.last[:BinaryData].should == nil
  end
  
  it "should send the $top query parameter when calling the service with top option " do
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
    expected_url = "http://myaccount.tables.core.windows.net/Customers()?$filter=expression&$top=1"
    expected_query = { '$filter' => "expression", '$top' => 1 }

    RestClient::Request.any_instance.expects(:execute).once().returns(mock_response)
    @table_service.expects(:generate_request_uri).once().with("Customers()", expected_query).returns(expected_url)
    @table_service.expects(:generate_request).once().with(:get, expected_url, expected_headers, nil).returns(RestClient::Request.new(:method => :post, :url => expected_url, :headers => expected_headers))
    entities = @table_service.query('Customers', {:expression => 'expression', :top => 1 })

    entities.length.should == 1
    entities.continuation_token[:next_partition_key].nil?.should == true
    entities.continuation_token[:next_row_key].nil?.should == true        
  end
  
  it "should return a continuation token as array property" do
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
    mock_response, mock_response2 = sample_feed
    mock_response.stubs(:headers).returns({:x_ms_continuation_nextpartitionkey => 'next_partition_key_value', :x_ms_continuation_nextrowkey => 'next_row_key_value'})
    
    expected_headers = {'Date' => Time.new.httpdate, 'DataServiceVersion' => '1.0;NetFx', 'Content-Type' => 'application/atom+xml', 'MaxDataServiceVersion' => '1.0;NetFx'}
    expected_query = { '$filter' => "expression" }    
    rest_client = RestClient::Request.new(:method => :post, :url => "http://myaccount.tables.core.windows.net/Customers()?$filter=expression", :headers => expected_headers)
    rest_client.expects(:execute).once().returns(mock_response)

    @table_service.expects(:generate_request_uri).with("Customers()", expected_query).returns("http://myaccount.tables.core.windows.net/Customers()?$filter=expression")
    @table_service.expects(:generate_request).once().with(:get, "http://myaccount.tables.core.windows.net/Customers()?$filter=expression", expected_headers, nil).returns(rest_client)
    entities = @table_service.query('Customers', {:expression => 'expression'})

   entities.length.should == 1
   entities.continuation_token['NextPartitionKey'].should == 'next_partition_key_value'
   entities.continuation_token['NextRowKey'].should == 'next_row_key_value'
  end  
  
  it "should throw when invalid table name is provided" do
    lambda { @table_service.create_table('9existing') }.should raise_error(WAZ::Tables::InvalidTableName)
  end
  
  it "should throw when invalid table name is provided" do
    lambda { @table_service.delete_table('9existing') }.should raise_error(WAZ::Tables::InvalidTableName)
  end

  it "should throw when invalid table name is provided" do
    lambda { @table_service.get_table('9existing') }.should raise_error(WAZ::Tables::InvalidTableName)
  end

  it "should throw when invalid table name is provided" do
    lambda { @table_service.insert_entity('9existing', 'entity') }.should raise_error(WAZ::Tables::InvalidTableName)
  end
  
  it "should throw when invalid table name is provided" do
    lambda { @table_service.delete_entity('9existing', 'foo', 'foo') }.should raise_error(WAZ::Tables::InvalidTableName)
  end
  
  it "should throw when invalid table name is provided" do
    lambda { @table_service.get_entity('9existing', 'foo', 'foo') }.should raise_error(WAZ::Tables::InvalidTableName)
  end
  
  it "should throw when invalid table name is provided" do
    lambda { @table_service.query('9existing', 'foo') }.should raise_error(WAZ::Tables::InvalidTableName)
  end
  
  it "should throw when invalid table name is provided" do
    lambda { @table_service.update_entity('9existing', {}) }.should raise_error(WAZ::Tables::InvalidTableName)
  end
  
  it "should throw when invalid table name is provided" do
    lambda { @table_service.merge_entity('9existing', {}) }.should raise_error(WAZ::Tables::InvalidTableName)
  end
end