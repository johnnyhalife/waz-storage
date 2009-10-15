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
require 'lib/waz-queues'

describe "storage service core behavior" do 
  it "should generate URI with given operation" do
    service = WAZ::Queues::Service.new("mock-account", "mock-key", "queue", true, "localhost")
    service.generate_request_uri(nil, :comp => 'list').should == "https://mock-account.queue.localhost/?comp=list"
  end
  
  it "should generate an URI without operation when operation is not given" do
    service = WAZ::Queues::Service.new("mock-account", "mock-key", "queue", true, "localhost")
    service.generate_request_uri("queue").should == "https://mock-account.queue.localhost/queue"
  end
  
  it "should generate a safe URI when path includes forward slash" do
    service = WAZ::Queues::Service.new("mock-account", "mock-key", "queue", true, "localhost")
    service.generate_request_uri("/queue").should == "https://mock-account.queue.localhost/queue"
  end
  
  it "should include additional parameters when given" do
    service = WAZ::Queues::Service.new("mock-account", "mock-key", "queue", true, "localhost")
    service.generate_request_uri("/queue", {:comp => 'list', :prefix => "p"}).should == "https://mock-account.queue.localhost/queue?comp=list&prefix=p"
  end

  it "should include additional parameters when given althought when there is no comp" do
    service = WAZ::Queues::Service.new("mock-account", "mock-key", "queue", true, "localhost")
    service.generate_request_uri("/queue", {:prefix => "p", :other => "other"}).should == "https://mock-account.queue.localhost/queue?other=other&prefix=p"
  end
  
  it "should canonicalize headers (order lexicographical, trim values, and join by NEW_LINES)" do
    service = WAZ::Queues::Service.new("mock-account", "mock-key", true, "localhost")
    headers = { "Content-Type" => "application/xml",
                "x-ms-prop-z" => "p",
                "x-ms-meta-name" => "a ",
                "x-other" => "other"}

    service.canonicalize_headers(headers).should == "x-ms-meta-name:a\nx-ms-prop-z:p"
  end
  
  it "should return empty string when no MS headers" do
    service = WAZ::Queues::Service.new("mock-account", "mock-key", true, "localhost")
    headers = { "Content-Type" => "application/xml",
                "x-other" => "other"}

    service.canonicalize_headers(headers).should == ""
  end
  
  it "should cannonicalize message by appending account_name to the request path" do
    service = WAZ::Queues::Service.new("mock-account", "mock-key", true, "localhost")
    service.canonicalize_message("http://localhost/queue?comp=list").should == "/mock-account/queue?comp=list"
  end
  
  it "should ignore every other querystring parameter rather than comp=" do
    service = WAZ::Queues::Service.new("mock-account", "mock-key", true, "localhost")
    service.canonicalize_message("http://localhost/queue?myparam=1").should == "/mock-account/queue"
  end
  
  it "should properly canonicalize message when no parameter associated with it" do
    service = WAZ::Queues::Service.new("mock-account", "mock-key", true, "localhost")
    service.canonicalize_message("http://mock-account.queue.core.windows.net/").should == "/mock-account/"
  end
  
  it "should properly canonicalize message when a resource is associated with it" do
    service = WAZ::Queues::Service.new("mock-account", "mock-key", true, "localhost")
    service.canonicalize_message("http://mock-account.queue.core.windows.net/resource?comp=list").should == "/mock-account/resource?comp=list"
  end
  
  it "should generate request with proper headers" do
    mock_request = RestClient::Request.new(:method => :put, :url => "http://localhost/johnny", :payload => "payload")
    mock_time = Time.new
    RestClient::Request.stubs(:new).with(:method => :put, :url => "http://localhost/johnny", :headers => {}, :payload => "payload").returns(mock_request)
    Time.stubs(:new).returns(mock_time)
    service = WAZ::Queues::Service.new("mock-account", "mock-key", true, "localhost")
    # mock the generate signature method since we want to assert against a know value 
    service.expects(:generate_signature).with(mock_request).returns("a_mock_signature")
    
    request = service.generate_request("PUT", "http://localhost/johnny", nil, "payload")
    request.headers["x-ms-Date"].should == mock_time.httpdate
    request.headers["Content-Length"].should == "payload".length
    request.headers["Authorization"] = "SharedKey mock-account:a_mock_signature"
  end
  
  it "should set content length when it is not provided" do
    mock_request = RestClient::Request.new(:method => :put, :url => "http://localhost/johnny")
    mock_time = Time.new
    RestClient::Request.stubs(:new).with(:method => :put, :url => "http://localhost/johnny", :headers => {}, :payload => nil).returns(mock_request)
    Time.stubs(:new).returns(mock_time)
    service = WAZ::Queues::Service.new("mock-account", "mock-key", true, "localhost")
    # mock the generate signature method since we want to assert against a know value 
    service.expects(:generate_signature).with(mock_request).returns("a_mock_signature")
    
    request = service.generate_request("PUT", "http://localhost/johnny", nil)
    request.headers["x-ms-Date"].should == mock_time.httpdate
    request.headers["Content-Length"].should == 0
    request.headers["Authorization"] = "SharedKey mock-account:a_mock_signature"
  end
  
  it "should name headers properly when they are provided as symbols" do
    mock_request = RestClient::Request.new(:method => :put, :url => "http://localhost/johnny")
    mock_time = Time.new
    RestClient::Request.stubs(:new).with(:method => :put, :url => "http://localhost/johnny", :headers => {"Content-Type" => "plain/xml"}, :payload => nil).returns(mock_request)
    Time.stubs(:new).returns(mock_time)
    service = WAZ::Queues::Service.new("mock-account", "mock-key", true, "localhost")
    # mock the generate signature method since we want to assert against a know value 
    service.expects(:generate_signature).with(mock_request).returns("a_mock_signature")
    
    request = service.generate_request("PUT", "http://localhost/johnny", {:Content_Type => "plain/xml"})
    request.headers["x-ms-Date"].should == mock_time.httpdate
    request.headers["Content-Length"].should == 0
    request.headers["Authorization"] = "SharedKey mock-account:a_mock_signature"
  end
end