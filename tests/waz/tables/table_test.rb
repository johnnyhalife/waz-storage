# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')

require 'rubygems'
require 'spec'
require 'mocha'
require 'restclient'
require 'lib/waz-tables'

describe "Table object behavior" do
  it "should list tables" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my-account", :access_key => "key"})
    result = [ {:name => 'table1', :url => 'url1'}, {:name => 'table2', :url => 'url2'} ], nil 
    WAZ::Tables::Service.any_instance.expects(:list_tables).returns(result)
    tables = WAZ::Tables::Table.list
    tables.size.should == 2
    tables.first().name.should == "table1"
    tables.first().url.should == "url1"    
    tables.last().name.should == "table2"
    tables.last().url.should == "url2"    
  end
  
  it "should create table" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my-account", :access_key => "key"})
    WAZ::Tables::Service.any_instance.expects(:create_table)
    table = WAZ::Tables::Table.create('table1') 
    table.name.should == "table1"
  end
  
  it "should destroy a table" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Tables::Service.any_instance.expects(:delete_table).with("table-to-delete")
    table = WAZ::Tables::Table.new(:name => 'table-to-delete')
    table.destroy!
  end
  
  it "should throw when not name provided for the table" do
    lambda { WAZ::Tables::Table.new({:foo => "bar"}) }.should raise_error(WAZ::Storage::InvalidOption)
  end
  
  it "should raise an exception when table name starts with - (hypen)" do
    lambda { WAZ::Tables::Table.create('-table')  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should raise an exception when table name  ends with - (hypen)" do
    lambda { WAZ::Tables::Table.create('table-')  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should raise an exception when table name is less than 3" do
    lambda { WAZ::Tables::Table.create('t')  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should raise an exception when table name is longer than 63" do
    lambda { WAZ::Tables::Table.create('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
end