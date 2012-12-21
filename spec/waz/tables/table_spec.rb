# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')
require 'tests/configuration'
require 'lib/waz-tables'

describe "Table object behavior" do
  it "should initialize a new table" do
    table = WAZ::Tables::Table.new({:name => 'tablename', :url => 'http://localhost' })
    table.name.should == 'tablename'
    table.url.should == 'http://localhost'
  end
  
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
  
  it "should find a table by its name and return a WAZ::Tables::Table instance" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my-account", :access_key => "key"})     
    WAZ::Tables::Service.any_instance.expects(:get_table).with('table1').returns({:name => 'table1', :url => 'url1'})
    table = WAZ::Tables::Table.find('table1')
    table.name.should == "table1"
    table.url.should == "url1"    
  end
  
  it "should return nil when looking for an unexisting table" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my-account", :access_key => "key"})     
    WAZ::Tables::Service.any_instance.expects(:get_table).with('unexistingtable').raises(WAZ::Tables::TableDoesNotExist.new('unexistingtable'))
    table = WAZ::Tables::Table.find('unexistingtable')
    table.nil?.should == true  
  end
  
  it "should create table" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my-account", :access_key => "key"})
    WAZ::Tables::Service.any_instance.expects(:create_table).returns({:name => 'table1', :url => 'http://foo'})
    table = WAZ::Tables::Table.create('table1') 
    table.name.should == "table1"
  end
  
  it "should destroy a table" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Tables::Service.any_instance.expects(:delete_table).with("tabletodelete")
    WAZ::Tables::Service.any_instance.expects(:get_table).returns({:name => 'tabletodelete', :url => 'http://localhost'})
    table = WAZ::Tables::Table.find('tabletodelete')
    table.destroy!
  end
  
  it "should throw when not name provided for the table" do
    lambda { WAZ::Tables::Table.new({:foo => "bar"}) }.should raise_error(WAZ::Storage::InvalidOption)
  end
  
  it "should raise an exception when table name starts with no lower/upper char" do
    lambda { WAZ::Tables::Table.create('9table')  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should raise an exception when table contains any other char than letters or digits" do
    lambda { WAZ::Tables::Table.create('table-name')  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should raise an exception when table name is less than 3" do
    lambda { WAZ::Tables::Table.create('t')  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should raise an exception when table name is longer than 63" do
    lambda { WAZ::Tables::Table.create('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end

  it "should raise an exception when :url option is not provided" do
    lambda { WAZ::Tables::Table.new({:name => 'name'})  }.should raise_error(WAZ::Storage::InvalidOption)
  end
  
  it "should raise an exception when :name option is not provided" do
    lambda { WAZ::Tables::Table.new({:url => 'url'})  }.should raise_error(WAZ::Storage::InvalidOption)
  end
  
  it "should raise an exception when :name is empty" do
    lambda { WAZ::Tables::Table.new({:name => '', :url => 'url'})  }.should raise_error(WAZ::Storage::InvalidOption)
  end
  
  it "should raise an exception when :url is empty" do
    lambda { WAZ::Tables::Table.new({:name => 'name', :url => ''})  }.should raise_error(WAZ::Storage::InvalidOption)
  end
    
  it "should raise an exception when invalid table name is provided" do
    INVALID_TABLE_ERROR_MESSAGE = "must start with at least one lower/upper characted, can have character or any digit starting from the second position, must be from 3 through 63 characters long"
    options = {:name => '1invalidname', :url => 'url'}
    options.stubs(:keys).returns([:name, :url])
    WAZ::Tables::Table.any_instance.stubs(:new).with(options).raises(WAZ::Storage::InvalidParameterValue)
    lambda { WAZ::Tables::Table.new(options)  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
end