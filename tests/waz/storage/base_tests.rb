# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')
require 'tests/configuration'
require 'lib/waz-blobs'

describe "Base class for connection management" do
  it "should throw an exception when it is not connected" do
    lambda {WAZ::Storage::Base.default_connection}.should raise_error(WAZ::Storage::NotConnected)
  end
  
  it "establish connection and set it as default connection" do
    WAZ::Storage::Base.establish_connection!(:account_name => 'myAccount',
                                             :access_key => "accountKey",
                                             :use_ssl => true)

    connection = WAZ::Storage::Base.default_connection                                     
    connection[:account_name].should == "myAccount"
    connection[:access_key].should == "accountKey"
    connection[:use_ssl].should == true
  end
  
  it "should throw an exception when no account_name is provided" do
    lambda {WAZ::Storage::Base.establish_connection!(:access_key => "accountKey", :use_ssl => false)}.should raise_error(WAZ::Storage::InvalidOption)
  end
  
  it "should throw an exception when no access_key is provided and no use_sas_auth_only" do
    lambda {WAZ::Storage::Base.establish_connection!(:account_name => "my_account", :use_ssl => false)}.should raise_error(WAZ::Storage::InvalidOption)
  end

  it "should throw and exception when use_sas_auth_only is provided but no sharedaccessignature" do
    lambda {WAZ::Storage::Base.establish_connection!(:account_name => "my_account",:use_sas_auth_only => true, :use_ssl => false)}.should raise_error(WAZ::Storage::InvalidOption)
  end

  
  it "should set use_ssl to false when no paramter provided" do
    WAZ::Storage::Base.establish_connection!(:account_name => 'myAccount',
                                           :access_key => "accountKey")

    connection = WAZ::Storage::Base.default_connection                                     
    connection[:account_name].should == "myAccount"
    connection[:access_key].should == "accountKey"
    connection[:use_ssl].should == false
  end

  it "should set :use_sas_auth_only to false when no parameter provided" do
    WAZ::Storage::Base.establish_connection!(:account_name => 'myAccount',
						:access_key => "accountKey")
    connection = WAZ::Storage::Base.default_connection
    connection[:account_name].should == "myAccount"
    connection[:access_key].should == "accountKey"
    connection[:use_sas_auth_only] == false 
  end  


  it "should be able to tell whether it's connected or not" do
    WAZ::Storage::Base.establish_connection!(:account_name => 'myAccount',
                                           :access_key => "accountKey")
    
    WAZ::Storage::Base.connected?.should == true
  end  
 
  it "should be able to tell whters it's connected by shared access signature" do
    WAZ::Storage::Base.establish_connection!(:account_name => 'myAccount',:use_sas_auth_only=>true, :sharedaccesssignature=>"?se=2011-12-13T23%3A54%3A07Z&sr=c&si=escrituraPAC&sig=e%2Bz6wsGhVRg%2FaxWATiS8A4WjWU%2FbfWjWa6zJ%2BTQIPzM%3D")
    WAZ::Storage::Base.connected?.should == true
  end
 
  it "should be able manage scoped connections" do
    WAZ::Storage::Base.establish_connection!(:account_name => 'myAccount', :access_key => "accountKey")
    
    WAZ::Storage::Base.default_connection[:account_name].should == 'myAccount'

    block_executed = false
    WAZ::Storage::Base.establish_connection(:account_name => 'otherAccount', :access_key => "accountKey") do
      WAZ::Storage::Base.default_connection[:account_name].should == 'otherAccount'
      block_executed = true
    end
    block_executed.should == true
    
    WAZ::Storage::Base.default_connection[:account_name].should == 'myAccount'    
  end
end
