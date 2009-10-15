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
require 'lib/waz-blobs'


describe "blobs service behavior" do
  it "should create container" do
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    RestClient::Request.any_instance.expects(:execute)
    service.expects(:generate_request_uri).with("mock-container").returns("mock-uri")
    service.expects(:generate_request).with("PUT", "mock-uri").returns(RestClient::Request.new(:method => "PUT", :url => "http://localhost"))
    service.create_container('mock-container')
  end
   
  it "should get container properties" do
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    mock_response = mock()
    mock_response.stubs(:headers).returns(RestClient::Response.beautify_headers({"x-ms-meta-Name" => "customName"}))
    RestClient::Request.any_instance.expects(:execute).returns(mock_response)
    service.expects(:generate_request_uri).with("mock-container").returns("mock-uri")
    service.expects(:generate_request).with("GET", "mock-uri").returns(RestClient::Request.new(:method => "GET", :url => "http://localhost"))
    properties = service.get_container_properties('mock-container')
    properties[:x_ms_meta_Name].should == "customName"
  end
  
  it "should set container properties" do
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    RestClient::Request.any_instance.expects(:execute)
    service.expects(:generate_request_uri).with("mock-container", {:comp => 'metadata'}).returns("mock-uri")
    service.expects(:generate_request).with("PUT", "mock-uri", {:x_ms_meta_Name => "myName"}).returns(RestClient::Request.new(:method => "PUT", :url => "http://localhost"))
    properties = service.set_container_properties('mock-container', {:x_ms_meta_Name => "myName"})
  end
  
  it "should get container acl" do
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    mock_response = mock()
    mock_response.stubs(:headers).returns(RestClient::Response.beautify_headers({"x-ms-prop-publicaccess" => true.to_s}))
    RestClient::Request.any_instance.expects(:execute).returns(mock_response)
    service.expects(:generate_request_uri).with("mock-container", {:comp => 'acl'}).returns("mock-uri")
    service.expects(:generate_request).with("GET", "mock-uri").returns(RestClient::Request.new(:method => "GET", :url => "http://localhost"))
    service.get_container_acl('mock-container').should == true
  end
  
  it "should set container acl" do
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    RestClient::Request.any_instance.expects(:execute)
    service.expects(:generate_request_uri).with("mock-container", :comp => 'acl').returns("mock-uri")
    service.expects(:generate_request).with("PUT", "mock-uri", {"x-ms-prop-publicaccess" => "false"}).returns(RestClient::Request.new(:method => "PUT", :url => "http://localhost"))
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
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with(nil, {:comp => 'list'}).returns("mock-uri")
    service.expects(:generate_request).with("GET", "mock-uri").returns(RestClient::Request.new(:method => "GET", :url => "http://localhost"))
    containers = service.list_containers
    containers[0][:name].should == "mycontainer"
    containers[1][:name].should == "othercontainer"    
  end
  
  it "should delete container" do
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    RestClient::Request.any_instance.expects(:execute)
    service.expects(:generate_request_uri).with("mock-container").returns("mock-uri")
    service.expects(:generate_request).with("DELETE", "mock-uri").returns(RestClient::Request.new(:method => "PUT", :url => "http://localhost"))
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
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with("container", :comp => 'list').returns("mock-uri")
    service.expects(:generate_request).with("GET", "mock-uri").returns(RestClient::Request.new(:method => "GET", :url => "http://localhost"))
    blobs = service.list_blobs("container")
    blobs[0][:name].should == "blob"
    blobs[1][:name].should == "blob2"
    blobs[0][:url].should == "http://localhost/container/blob"
    blobs[1][:url].should == "http://localhost/container/blob2"
    blobs[0][:content_type].should == "text/xml"
    blobs[1][:content_type].should == "application/x-stream"
  end
  
  it "should put blob" do
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    RestClient::Request.any_instance.expects(:execute).returns(nil)
    service.expects(:generate_request_uri).with("container/blob").returns("mock-uri")
    service.expects(:generate_request).with("PUT", "mock-uri", {'Content-Type' => 'application/octet-stream'}, "payload").returns(RestClient::Request.new(:method => "PUT", :url => "http://localhost"))
    service.put_blob("container/blob", "payload")
  end
  
  it "should get blob" do
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    RestClient::Request.any_instance.expects(:execute).returns("payload")
    service.expects(:generate_request_uri).with("container/blob").returns("mock-uri")
    service.expects(:generate_request).with("GET", "mock-uri").returns(RestClient::Request.new(:method => "GET", :url => "http://localhost"))
    service.get_blob("container/blob").should == "payload"
  end
  
  it "should delete blob" do
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    RestClient::Request.any_instance.expects(:execute).returns(nil)
    service.expects(:generate_request_uri).with("container/blob").returns("mock-uri")
    service.expects(:generate_request).with("DELETE", "mock-uri").returns(RestClient::Request.new(:method => "PUT", :url => "http://localhost"))
    service.delete_blob("container/blob")
  end
  
  it "should get blob properties" do
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    response = mock()
    response.stubs(:headers).returns(RestClient::Response.beautify_headers({"x-ms-meta-Name" => "customName"}))
    RestClient::Request.any_instance.expects(:execute).returns(response)
    service.expects(:generate_request_uri).with("container/blob").returns("mock-uri")
    service.expects(:generate_request).with("HEAD", "mock-uri").returns(RestClient::Request.new(:method => "GET", :url => "http://localhost"))
    service.get_blob_properties("container/blob").should == response.headers
  end
  
  it "should set blob properties" do
    service = WAZ::Blobs::Service.new("mock-account", "mock-key")
    RestClient::Request.any_instance.expects(:execute).returns(nil)
    service.expects(:generate_request_uri).with("container/blob", :comp => 'metadata').returns("mock-uri")
    service.expects(:generate_request).with("PUT", "mock-uri", {:x_ms_meta_Name => "johnny"}).returns(RestClient::Request.new(:method => "GET", :url => "http://localhost"))
    service.set_blob_properties("container/blob", {:x_ms_meta_Name => "johnny"})
  end
end