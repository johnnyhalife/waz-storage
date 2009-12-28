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
    <?xml version="1.0" encoding="utf-8" standalone="yes"?>
    <entry xml:base="http://wazstoragejohnny.table.core.windows.net/" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns="http://www.w3.org/2005/Atom">
      <id>http://myaccount.table.core.windows.net/Tables('table1')</id>
      <title type="text"></title>
      <updated>2009-12-28T02:00:21Z</updated>
      <author>
        <name />
      </author>
      <link rel="edit" title="Tables" href="Tables('table1')" />
      <category term="wazstoragejohnny.Tables" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
      <content type="application/xml">
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
    service.expects(:generate_request).with(:get, "http://localhost/Tables('table1')", {'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx'}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost/Tables('table1')"))
    table = service.get_table('table1')
    table[:name].should == 'table1'
    table[:url].should == "http://myaccount.table.core.windows.net/Tables('table1')"    
  end  
  it "should list all tables" do
    response = <<-eos
    <?xml version="1.0" encoding="utf-8" standalone="yes"?>
    <feed xml:base="http://myaccount.tables.core.windows.net/" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns="http://www.w3.org/2005/Atom">
      <title type="text">Tables</title>
      <id>http://myaccount.tables.core.windows.net/Tables</id>
      <updated>2009-01-04T17:18:54.7062347Z</updated>
      <link rel="self" title="Tables" href="Tables" />
      <entry>
        <id>http://myaccount.tables.core.windows.net/Tables('table1')</id>
        <title type="text"></title>
        <updated>2009-01-04T17:18:54.7062347Z</updated>
        <author>
          <name />
        </author>
        <link rel="edit" title="Tables" href="Tables('table1')" />
        <category term="myaccount.Tables" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
        <content type="application/xml">
          <m:properties>
            <d:TableName>table1</d:TableName>
          </m:properties>
        </content>
      </entry>
      <entry>
        <id>http://myaccount.tables.core.windows.net/Tables('table2')</id>
        <title type="text"></title>
        <updated>2009-01-04T17:18:54.7062347Z</updated>
        <author>
          <name />
        </author>
        <link rel="edit" title="Tables" href="Tables('table2')" />
        <category term="myaccount.Tables" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" />
        <content type="application/xml">
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
    service.expects(:generate_request).with(:get, "http://localhost/Tables", {'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx'}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost/Tables"))
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
    service.expects(:generate_request).with(:get, "http://localhost/Tables?NextTableName=next-table-name", {'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx'}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost/Tables?NextTableName=next-table-name"))
    tables, next_table_name = service.list_tables('next-table-name')
  end
  
  it "should create a table" do
    expected_payload = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?><entry xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xmlns=\"http://www.w3.org/2005/Atom\"><title /><updated>#{Time.now.utc.iso8601}</updated><author><name/></author><id/><content type=\"application/xml\"><m:properties><d:TableName>mock-table</d:TableName></m:properties></content></entry>"
        
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute)
    service.expects(:generate_request_uri).with("Tables", {}).returns("http://localhost/Tables")
    service.expects(:generate_request).with(:post, "http://localhost/Tables", { 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }, expected_payload).returns(RestClient::Request.new(:method => :post, :url => "http://localhost/Tables"))
    service.create_table('mock-table')
  end
    
  it "should throw when a table already exists" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    failed_response = RestClient::RequestFailed.new
    failed_response.stubs(:http_code).returns(409)    
    RestClient::Request.any_instance.expects(:execute).raises(failed_response)
    lambda { service.create_table('existingtable') }.should raise_error(WAZ::Tables::TableAlreadyExists, "The table existingtable already exists on your account.")
  end
  
  it "should delete a table" do
    service = WAZ::Tables::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "table", :use_ssl => true, :base_url => "localhost")
    response = mock()
    response.stubs(:code).returns(204)
    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with("Tables('table-to-delete')", {}).returns("http://localhost/Tables('table-to-delete')")
    service.expects(:generate_request).with(:delete, "http://localhost/Tables('table-to-delete')", { 'Content-Type' => 'application/atom+xml', 'DataServiceVersion' => '1.0;NetFx', 'MaxDataServiceVersion' => '1.0;NetFx' }, nil).returns(RestClient::Request.new(:method => :delete, :url => "http://localhost/Tables('tabletodelete')"))
    service.delete_table('table-to-delete')
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
end