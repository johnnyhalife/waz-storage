# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')

require 'rubygems'
require 'spec'
require 'mocha'
require 'restclient'
require 'tests/configuration'
require 'lib/waz-blobs'

describe "Windows Azure Containers interface API" do 
  
  it "should should throw when no container name is provided" do
    lambda {WAZ::Blobs::Container.new}.should raise_error(WAZ::Storage::InvalidOption)
  end
  
  it "should list containers" do
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:list_containers).returns([{:name => 'my_container'}, {:name => 'other container'}])
    containers = WAZ::Blobs::Container.list
    containers.size.should == 2
    containers.first().name.should == "my_container"
  end
  
  it "should be able to create a container" do
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:create_container).with("my_container")
    container = WAZ::Blobs::Container.create('my_container')
    container.name.should == 'my_container'
  end
  
  it "should be able to return a container by name" do
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"}).twice
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container_name").returns({:name => "container_name", :x_ms_meta_name => "container_name"}).twice
    container = WAZ::Blobs::Container.find('container_name')
    container.metadata[:x_ms_meta_name].should == 'container_name'
  end
  
  it "should be able to return container metadata" do
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"}).twice()
    WAZ::Blobs::Service.any_instance.expects(:create_container).with("container_name")
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container_name").returns({:x_ms_meta_name => "container_name"})
    container = WAZ::Blobs::Container.create('container_name')
    container.metadata[:x_ms_meta_name].should == 'container_name'  
  end
  
  it "should be able to say whether the container is public or not" do
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"}).twice()
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container_name").returns({:x_ms_meta_name => "container_name"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_acl).with("container_name").returns(false)
    container = WAZ::Blobs::Container.find("container_name")
    container.public_access?.should == false
  end
  
  it "should be able to set whether the container is public or not" do
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"}).twice()
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container_name").returns({:x_ms_meta_name => "container_name"})
    WAZ::Blobs::Service.any_instance.expects(:set_container_acl).with('container_name', false)
    container = WAZ::Blobs::Container.find("container_name")
    container.public_access = false
  end
  
  it "should be able to set container properties" do
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"}).twice
    WAZ::Blobs::Service.any_instance.expects(:set_container_properties).with("container_name", {:x_ms_meta_meta1 => "meta1"}).returns(false)
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container_name").returns({:x_ms_meta_name => "container_name"})
    container = WAZ::Blobs::Container.find("container_name")
    container.put_properties!(:x_ms_meta_meta1 => "meta1")
  end
  
  it "should be able to return a list files within the container" do
    expected_blobs = [ {:name => 'blob1', :url => 'url', :content_type => 'app/xml'}, {:name => 'blob2', :url => 'url', :content_type => 'app/xml'} ]
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"}).at_least(2)
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container_name").returns({:x_ms_meta_name => "container_name"})
    WAZ::Blobs::Service.any_instance.expects(:list_blobs).with("container_name").returns(expected_blobs)
    container = WAZ::Blobs::Container.find("container_name")
    container_blobs = container.blobs
    container_blobs.first().name = expected_blobs[0][:name]
    container_blobs[1].name = expected_blobs[1][:name]
  end
  
  it "should destroy container" do
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"}).at_least(2)
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container_name").returns({:x_ms_meta_name => "container_name"})
    WAZ::Blobs::Service.any_instance.expects(:delete_container).with("container_name")
    container = WAZ::Blobs::Container.find("container_name")
    container.destroy!
  end
  
  it "should be able to return null when container not found by name" do
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container_name").raises(RestClient::ResourceNotFound)
    container = WAZ::Blobs::Container.find('container_name')
    container.nil?.should == true
  end
  
  it "should be able to put blob inside given container" do
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"}).at_most(3)
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container_name").returns({:x_ms_meta_name => "container_name"})
    WAZ::Blobs::Service.any_instance.expects(:put_blob).with("container_name/my_blob", "this is the blob content", "text/plain; charset=UTF-8", {:x_ms_meta_custom_property => "customValue"})
    container = WAZ::Blobs::Container.find("container_name")
    blob = container.store("my_blob", "this is the blob content", "text/plain; charset=UTF-8", {:x_ms_meta_custom_property => "customValue"})
    blob.name.should == "my_blob"
    blob.url.should == "http://my_account.blob.core.windows.net/container_name/my_blob"
    blob.content_type = "text/plain; charset=UTF-8"
  end
  
  it "should return a specific blob for the given container" do
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"}).at_most(3)
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container_name").returns({:x_ms_meta_name => "container_name"})
    WAZ::Blobs::Service.any_instance.expects(:get_blob_properties).with("container_name/my_blob").returns({ :content_type => "application/xml" })
    container = WAZ::Blobs::Container.find("container_name")
    blob = container['my_blob']
    blob.name.should == 'my_blob'
    blob.content_type.should == 'application/xml'
    blob.url.should == 'http://my_account.blob.core.windows.net/container_name/my_blob'
  end
  
  it "should return nil when the file does not exist" do
    WAZ::Storage::Base.expects(:default_connection).returns({:account_name => "my_account", :access_key => "key"}).at_most(2)
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container_name").returns({:x_ms_meta_name => "container_name"})
    WAZ::Blobs::Service.any_instance.expects(:get_blob_properties).with("container_name/my_blob").raises(RestClient::ResourceNotFound)
    container = WAZ::Blobs::Container.find('container_name')
    blob = container['my_blob']
    blob.nil?.should == true
  end
end