# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')
require 'tests/configuration'
require 'lib/waz-blobs'

describe "blobs service behavior" do
  it "should create container" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute)
    service.expects(:generate_request_uri).with("mock-container", {:restype => 'container'}).returns("mock-uri")
    service.expects(:generate_request).with(:put, "mock-uri", {:x_ms_version => '2009-09-19'}, nil).returns(RestClient::Request.new(:method => :put, :url => "http://localhost"))
    service.create_container('mock-container')
  end
   
  it "should get container properties" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    mock_response = mock()
    mock_response.stubs(:headers).returns(RestClient::Response.beautify_headers({"x-ms-meta-Name" => "customName"}))
    RestClient::Request.any_instance.expects(:execute).returns(mock_response)
    service.expects(:generate_request_uri).with("mock-container", {:restype => 'container'}).returns("mock-uri")
    service.expects(:generate_request).with(:get, "mock-uri", {:x_ms_version => '2009-09-19'}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost"))
    properties = service.get_container_properties('mock-container')
    properties[:x_ms_meta_name].should == "customName"
  end
  
  it "should set container properties" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute)
    service.expects(:generate_request_uri).with("mock-container", {:restype => 'container', :comp => 'metadata'}).returns("mock-uri")
    service.expects(:generate_request).with(:put, "mock-uri", {:x_ms_version => '2009-09-19', :x_ms_meta_Name => "myName"}, nil).returns(RestClient::Request.new(:method => :put, :url => "http://localhost"))
    properties = service.set_container_properties('mock-container', {:x_ms_meta_Name => "myName"})
  end
  
  it "should get container acl" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    mock_response = mock()
    mock_response.stubs(:headers).returns(RestClient::Response.beautify_headers({"x-ms-prop-publicaccess" => true.to_s}))
    RestClient::Request.any_instance.expects(:execute).returns(mock_response)
    service.expects(:generate_request_uri).with("mock-container", {:restype => 'container', :comp => 'acl'}).returns("mock-uri")
    service.expects(:generate_request).with(:get, "mock-uri", {:x_ms_version => '2009-09-19'}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost"))
    service.get_container_acl('mock-container').should == true
  end
  
  it "should set container acl" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute)
    service.expects(:generate_request_uri).with("mock-container", :restype => 'container', :comp => 'acl').returns("mock-uri")
    service.expects(:generate_request).with(:put, "mock-uri", {:x_ms_version => '2009-09-19', :x_ms_prop_publicaccess => "false"}, nil).returns(RestClient::Request.new(:method => :put, :url => "http://localhost"))
    properties = service.set_container_acl('mock-container', false)
  end
  
  it "should list containers" do
    response = <<-eos
                <?xml version="1.0" encoding="utf-8"?>
                <EnumerationResults AccountName="http://myaccount.blob.core.windows.net">
                  <Containers>
                    <Container>
                      <Name>mycontainer</Name>
                      <Url>http://localhost/mycontainer</Url>
                      <LastModified>2009-09-11</LastModified>
                    </Container>
                    <Container>
                      <Name>othercontainer</Name>
                      <Url>http://localhost/othercontainer</Url>
                      <LastModified>2009-09-11</LastModified>
                    </Container>
                  </Containers>
                </EnumerationResults>
                eos
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with(nil, {:comp => 'list'}).returns("mock-uri")
    service.expects(:generate_request).with(:get, "mock-uri", {}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost"))
    containers = service.list_containers
    containers[0][:name].should == "mycontainer"
    containers[1][:name].should == "othercontainer"    
  end
  
  it "should delete container" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute)
    service.expects(:generate_request_uri).with("mock-container", {:restype => 'container'}).returns("mock-uri")
    service.expects(:generate_request).with(:delete, "mock-uri", {:x_ms_version => '2009-09-19'}, nil).returns(RestClient::Request.new(:method => :put, :url => "http://localhost"))
    service.delete_container('mock-container')
  end
  
  it "should list blobs" do
    response = <<-eos
                <?xml version="1.0" encoding="utf-8"?>
                <EnumerationResults AccountName="http://myaccount.blob.core.windows.net">
                 <Blobs>
                     <Blob>
                       <Url>http://localhost/container/blob</Url>
                       <Name>blob</Name>
                       <ContentType>text/xml</ContentType>
                     </Blob>
                     <Blob>
                       <Url>http://localhost/container/blob2</Url>
                       <Name>blob2</Name>
                       <ContentType>application/x-stream</ContentType>
                     </Blob>
                   </Blobs>
                </EnumerationResults>
                eos
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with("container", {:comp => 'list'}).returns("mock-uri")
    service.expects(:generate_request).with(:get, "mock-uri", {}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost"))
    blobs = service.list_blobs("container")
    blobs[0][:name].should == "blob"
    blobs[1][:name].should == "blob2"
    blobs[0][:url].should == "http://localhost/container/blob"
    blobs[1][:url].should == "http://localhost/container/blob2"
    blobs[0][:content_type].should == "text/xml"
    blobs[1][:content_type].should == "application/x-stream"
  end
  
  it "should put blob" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(nil)
    service.expects(:generate_request_uri).with("container/blob", nil).returns("mock-uri")
    service.expects(:generate_request).with(:put, "mock-uri", {'Content-Type' => 'application/octet-stream', :x_ms_version => "2009-09-19", :x_ms_blob_type => "BlockBlob"}, "payload").returns(RestClient::Request.new(:method => :put, :url => "http://localhost"))
    service.put_blob("container/blob", "payload")
  end
  
  it "should get blob" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns("payload")
    service.expects(:generate_request_uri).with("container/blob", {}).returns("mock-uri")
    service.expects(:generate_request).with(:get, "mock-uri", {:x_ms_version => "2009-09-19"}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost"))
    service.get_blob("container/blob").should == "payload"
  end
  
  it "should delete blob" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(nil)
    service.expects(:generate_request_uri).with("container/blob", nil).returns("mock-uri")
    service.expects(:generate_request).with(:delete, "mock-uri", {:x_ms_version => "2009-09-19"}, nil).returns(RestClient::Request.new(:method => :put, :url => "http://localhost"))
    service.delete_blob("container/blob")
  end
  
  it "should get blob properties" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    response = mock()
    response.stubs(:headers).returns(RestClient::Response.beautify_headers({"x-ms-meta-Name" => "customName"}))
    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with("container/blob", {}).returns("mock-uri")
    service.expects(:generate_request).with(:head, "mock-uri", {:x_ms_version => '2009-09-19'}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost"))
    service.get_blob_properties("container/blob").should == response.headers
  end
  
  it "should set blob properties" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(nil)
    service.expects(:generate_request_uri).with("container/blob", :comp => 'properties').returns("mock-uri")
    service.expects(:generate_request).with(:put, "mock-uri", {:x_ms_version => '2009-09-19', :x_ms_meta_Name => "johnny"}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost"))
    service.set_blob_properties("container/blob", {:x_ms_meta_Name => "johnny"})
  end
  
  it "should copy blob" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(nil)
    service.expects(:generate_request_uri).with("container/blob-copy", nil).returns("mock-uri")
    service.expects(:generate_request).with(:put, "mock-uri", {:x_ms_version => "2009-09-19", :x_ms_copy_source => "/mock-account/container/blob"}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost"))
    service.copy_blob("container/blob", "container/blob-copy")
  end
  
  it "should put block" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(nil)
    service.expects(:generate_request_uri).with("container/blob", { :blockid => 'block_id', :comp => 'block'}).returns("mock-uri")
    service.expects(:generate_request).with(:put, "mock-uri", {'Content-Type' => 'application/octet-stream'}, "payload").returns(RestClient::Request.new(:method => :get, :url => "http://localhost"))
    service.put_block("container/blob", "block_id", "payload")
  end 
  
  it "should list blocks" do
    response = <<-eos
                <?xml version="1.0" encoding="utf-8"?>
                <BlockList>
                  <CommittedBlocks>
                    <Block>
                      <Name>AAAAAA==</Name>
                      <Size>1048576</Size>
                    </Block>
                  </CommittedBlocks>
                  <UncommittedBlocks>
                    <Block>
                      <Name>AQAAAA==</Name>
                      <Size>1048576</Size>
                    </Block>
                    <Block>
                      <Name>AgAAAA==</Name>
                      <Size>402848</Size>
                    </Block>
                  </UncommittedBlocks>
                </BlockList>
                eos
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with('container/blob', {:comp => 'blocklist', :blocklisttype => 'all'}).returns("mock-uri")
    service.expects(:generate_request).with(:get, "mock-uri", {:x_ms_version => "2009-04-14"}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost"))
    blocks = service.list_blocks('container/blob')
    blocks.first[:name].should == "AAAAAA=="
    blocks.first[:size].should == "1048576"
    blocks.first[:committed].should == true
    blocks.last[:name].should == "AgAAAA=="
    blocks.last[:size].should == "402848"
    blocks.last[:committed].should == false
  end
  
  it "should list with additional parameters" do
    response = <<-eos
                <?xml version="1.0" encoding="utf-8"?>
                <BlockList>
                  <CommittedBlocks>
                    <Block>
                      <Name>AAAAAA==</Name>
                      <Size>1048576</Size>
                    </Block>
                  </CommittedBlocks>
                  <UncommittedBlocks>
                    <Block>
                      <Name>AQAAAA==</Name>
                      <Size>1048576</Size>
                    </Block>
                    <Block>
                      <Name>AgAAAA==</Name>
                      <Size>402848</Size>
                    </Block>
                  </UncommittedBlocks>
                </BlockList>
                eos
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with('container/blob', {:comp => 'blocklist', :blocklisttype => 'committed'}).returns("mock-uri")
    service.expects(:generate_request).with(:get, "mock-uri", {:x_ms_version => "2009-04-14"}, nil).returns(RestClient::Request.new(:method => :get, :url => "http://localhost"))
    blocks = service.list_blocks('container/blob', 'COMMITTED')
    blocks.first[:name].should == "AAAAAA=="
    blocks.first[:size].should == "1048576"
    blocks.first[:committed].should == true
    blocks.last[:name].should == "AgAAAA=="
    blocks.last[:size].should == "402848"
    blocks.last[:committed].should == false
  end
  
  it "should throw when block list type is nil or doesn't fall into the valid values" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    lambda { service.list_blocks('container/blob', 'whatever') }.should raise_error(WAZ::Storage::InvalidParameterValue)
    lambda { service.list_blocks('container/blob', nil) }.should raise_error(WAZ::Storage::InvalidParameterValue)    
  end
  
  it "should take blob snapshots" do
    service = WAZ::Blobs::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    
    mock_response = mock()
    mock_response.stubs(:headers).returns({:x_ms_snapshot => Time.new.httpdate})
    mock_request = mock()
    mock_request.stubs(:execute).returns(mock_response)
    
    service.expects(:generate_request_uri).with("container/blob", {:comp => "snapshot"}).returns("container/blob")
    service.expects(:generate_request).with(:put, "container/blob", {:x_ms_version => "2009-09-19"}, nil).returns(mock_request)
    service.snapshot_blob("container/blob").should == mock_response.headers[:x_ms_snapshot]
  end
end