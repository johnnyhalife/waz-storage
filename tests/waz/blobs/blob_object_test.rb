# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')
require 'tests/configuration'
require 'lib/waz-blobs'

describe "Windows Azure Blobs interface API" do
  it "should return blob path from url" do
    blob = WAZ::Blobs::BlobObject.new(:name => "blob_name", :url => "http://localhost/container/blob", :content_type => "application/xml")  
    blob.path.should == "container/blob"
  end
  
  it "should return blob metdata" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_blob_properties).with("container/blob").returns({:x_ms_meta_name => "blob_name"})
    blob = WAZ::Blobs::BlobObject.new(:name => "blob_name", :url => "http://localhost/container/blob", :content_type => "application/xml")  
    blob.metadata.should == { :x_ms_meta_name => "blob_name" }
  end
  
  it "should put blob metadataa" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:set_blob_properties).with("container/blob", {:x_ms_meta_name => "blob_name"})
    blob = WAZ::Blobs::BlobObject.new(:name => "blob_name", :url => "http://localhost/container/blob", :content_type => "application/xml")  
    blob.put_properties!({ :x_ms_meta_name => "blob_name" })
  end
  
  it "should get blob contents" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_blob).with("container/blob").returns("this is the blob content")
    blob = WAZ::Blobs::BlobObject.new(:name => "blob_name", :url => "http://localhost/container/blob", :content_type => "application/xml")  
    blob.value.should == "this is the blob content"
  end
  
  it "should put blob contents" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
    WAZ::Blobs::Service.any_instance.expects(:get_blob_properties).with("container/blob").returns({})
    WAZ::Blobs::Service.any_instance.expects(:put_blob).with("container/blob", "my new blob value", "application/xml", {}).returns("this is the blob content")
    blob = WAZ::Blobs::BlobObject.new(:name => "blob_name", :url => "http://localhost/container/blob", :content_type => "application/xml")  
    blob.value = "my new blob value"
  end
  
  it "should destroy blob" do
   WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})
   WAZ::Blobs::Service.any_instance.expects(:delete_blob).with("container/blob")
    blob = WAZ::Blobs::BlobObject.new(:name => "blob_name", :url => "http://localhost/container/blob", :content_type => "application/xml")  
   blob.destroy!
  end
  
  it "should copy blob" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})   
    WAZ::Blobs::BlobObject.service_instance.expects(:copy_blob).with('container/blob', 'container/blob-copy')
    WAZ::Blobs::BlobObject.service_instance.expects(:get_blob_properties).with('container/blob-copy').returns(:content_type => "plain/text")
    blob = WAZ::Blobs::BlobObject.new(:name => "blob_name", :url => "http://localhost/container/blob", :content_type => "plain/text")  
    copy = blob.copy('container/blob-copy')
    copy.path.should == "container/blob-copy"
  end
  
  it "should take a snapshot of a blob" do
    mock_time = Time.new 
    Time.stubs(:new).returns(mock_time)
    
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my_account", :access_key => "key"})   
    WAZ::Blobs::BlobObject.service_instance.expects(:get_blob_properties).with('container/blob').returns(:content_type => "plain/text")
    WAZ::Blobs::BlobObject.service_instance.expects(:snapshot_blob).with('container/blob').returns(mock_time.httpdate)
    
    blob = WAZ::Blobs::BlobObject.new(:name => "blob_name", :url => "http://localhost/container/blob", :content_type => "plain/text")  
    blob_snapshot = blob.snapshot
    blob_snapshot.snapshot_date.should === mock_time.httpdate
  end
  
  it "should not allow snapshoted blobs to perform write operations" do    
    snapshot = WAZ::Blobs::BlobObject.new(:name => "blob_name", :url => "http://localhost/container/blob", :content_type => "plain/text", :snapshot_date => Time.new.httpdate)  
    lambda { snapshot.value = "new-value" }.should raise_error(WAZ::Blobs::InvalidOperation)    
    lambda { snapshot.put_properties!({:x_ms_meta_name => "foo"}) }.should raise_error(WAZ::Blobs::InvalidOperation)    
  end
  
  it "blob path should include snapshot parameter" do
    blob = WAZ::Blobs::BlobObject.new(:name => "blob_name", :url => "http://localhost/container/blob?snapshot=foo", :content_type => "application/xml")  
    blob.path.should == "container/blob?snapshot=foo"
  end
end