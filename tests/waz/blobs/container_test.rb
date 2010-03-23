# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')
require 'tests/configuration'
require 'lib/waz-blobs'

describe "Windows Azure Containers interface API" do 
  
  it "should should throw when no container name is provided" do
    lambda {WAZ::Blobs::Container.new}.should raise_error(WAZ::Storage::InvalidOption)
  end
  
  it "should list containers" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:list_containers).returns([{:name => 'my-container'}, {:name => 'other container'}])
    containers = WAZ::Blobs::Container.list
    containers.size.should == 2
    containers.first().name.should == "my-container"
  end
  
  it "should be able to create a container" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:create_container).with("my-container")
    container = WAZ::Blobs::Container.create('my-container')
    container.name.should == 'my-container'
  end
  
  it "should be able to return a container by name" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container-name").returns({:name => "container-name", :x_ms_meta_name => "container-name"}).twice
    container = WAZ::Blobs::Container.find('container-name')
    container.metadata[:x_ms_meta_name].should == 'container-name'
  end
  
  it "should be able to return container metadata" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:create_container).with("container-name")
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container-name").returns({:x_ms_meta_name => "container-name"})
    container = WAZ::Blobs::Container.create('container-name')
    container.metadata[:x_ms_meta_name].should == 'container-name'  
  end
  
  it "should be able to say whether the container is public or not" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container-name").returns({:x_ms_meta_name => "container-name"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_acl).with("container-name").returns(false)
    container = WAZ::Blobs::Container.find("container-name")
    container.public_access?.should == false
  end
  
  it "should be able to set whether the container is public or not" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container-name").returns({:x_ms_meta_name => "container-name"})
    WAZ::Blobs::Service.any_instance.expects(:set_container_acl).with('container-name', false)
    container = WAZ::Blobs::Container.find("container-name")
    container.public_access = false
  end
  
  it "should be able to set container properties" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:set_container_properties).with("container-name", {:x_ms_meta_meta1 => "meta1"}).returns(false)
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container-name").returns({:x_ms_meta_name => "container-name"})
    container = WAZ::Blobs::Container.find("container-name")
    container.put_properties!(:x_ms_meta_meta1 => "meta1")
  end
  
  it "should be able to return a list files within the container" do
    expected_blobs = [ {:name => 'blob1', :url => 'url', :content_type => 'app/xml'}, {:name => 'blob2', :url => 'url', :content_type => 'app/xml'} ]
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container-name").returns({:x_ms_meta_name => "container-name"})
    WAZ::Blobs::Service.any_instance.expects(:list_blobs).with("container-name").returns(expected_blobs)
    container = WAZ::Blobs::Container.find("container-name")
    container_blobs = container.blobs
    container_blobs.first().name = expected_blobs[0][:name]
    container_blobs[1].name = expected_blobs[1][:name]
  end
  
  it "should destroy container" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container-name").returns({:x_ms_meta_name => "container-name"})
    WAZ::Blobs::Service.any_instance.expects(:delete_container).with("container-name")
    container = WAZ::Blobs::Container.find("container-name")
    container.destroy!
  end
  
  it "should be able to return null when container not found by name" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container-name").raises(RestClient::ResourceNotFound)
    container = WAZ::Blobs::Container.find('container-name')
    container.nil?.should == true
  end
  
  it "should be able to put blob inside given container" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container-name").returns({:x_ms_meta_name => "container-name"})
    WAZ::Blobs::Service.any_instance.expects(:put_blob).with("container-name/my_blob", "this is the blob content", "text/plain; charset=UTF-8", {:x_ms_meta_custom_property => "customValue"})
    container = WAZ::Blobs::Container.find("container-name")
    blob = container.store("my_blob", "this is the blob content", "text/plain; charset=UTF-8", {:x_ms_meta_custom_property => "customValue"})
    blob.name.should == "my_blob"
    blob.url.should == "http://my_account.blob.core.windows.net/container-name/my_blob"
    blob.content_type = "text/plain; charset=UTF-8"
  end
  
  it "should be able to put blob inside given container (when simulating fake containers)" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container-name").returns({:x_ms_meta_name => "container-name"})
    WAZ::Blobs::Service.any_instance.expects(:put_blob).with("container-name/fake-container/blob", "this is the blob content", "text/plain; charset=UTF-8", {:x_ms_meta_custom_property => "customValue"})
    container = WAZ::Blobs::Container.find("container-name")
    blob = container.store("/fake-container/blob", "this is the blob content", "text/plain; charset=UTF-8", {:x_ms_meta_custom_property => "customValue"})
    blob.name.should == "fake-container/blob"
    blob.url.should == "http://my_account.blob.core.windows.net/container-name/fake-container/blob"
    blob.content_type = "text/plain; charset=UTF-8"
  end
  
  it "should return a specific blob for the given container" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container-name").returns({:x_ms_meta_name => "container-name"})
    WAZ::Blobs::Service.any_instance.expects(:get_blob_properties).with("container-name/my_blob").returns({ :content_type => "application/xml" })
    container = WAZ::Blobs::Container.find("container-name")
    blob = container['my_blob']
    blob.name.should == 'my_blob'
    blob.content_type.should == 'application/xml'
    blob.url.should == 'http://my_account.blob.core.windows.net/container-name/my_blob'
  end
  
  it "should return nil when the file does not exist" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_container_properties).with("container-name").returns({:x_ms_meta_name => "container-name"})
    WAZ::Blobs::Service.any_instance.expects(:get_blob_properties).with("container-name/my_blob").raises(RestClient::ResourceNotFound)
    container = WAZ::Blobs::Container.find('container-name')
    blob = container['my_blob']
    blob.nil?.should == true
  end
  
  it "should raise an exception when container name starts with - (hypen)" do
    lambda { WAZ::Blobs::Container.create('-container')  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should raise an exception when container name  ends with - (hypen)" do
    lambda { WAZ::Blobs::Container.create('container-')  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should raise an exception when container name is less than 3" do
    lambda { WAZ::Blobs::Container.create('co')  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end
  
  it "should raise an exception when container name is longer than 63" do
    lambda { WAZ::Blobs::Container.create('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')  }.should raise_error(WAZ::Storage::InvalidParameterValue)
  end

end