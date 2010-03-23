# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')
require 'tests/configuration'
require 'lib/waz-queues'

describe "storage service core behavior" do 
  it "should generate URI with given operation" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    service.generate_request_uri(nil, :comp => 'list').should == "https://mock-account.queue.localhost/?comp=list"
  end
  
  it "should generate an URI without operation when operation is not given" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    service.generate_request_uri("queue").should == "https://mock-account.queue.localhost/queue"
  end
  
  it "should generate a safe URI when path includes forward slash" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    service.generate_request_uri("/queue").should == "https://mock-account.queue.localhost/queue"
  end
  
  it "should include additional parameters when given" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    service.generate_request_uri("/queue", {:comp => 'list', :prefix => "p"}).should == "https://mock-account.queue.localhost/queue?comp=list&prefix=p"
  end

  it "should include additional parameters when given althought when there is no comp" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    service.generate_request_uri("/queue", {:prefix => "p", :other => "other"}).should == "https://mock-account.queue.localhost/queue?other=other&prefix=p"
  end
  
  it "should include additional parameters when given althought when there is no comp" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    service.generate_request_uri("/queue", {:comp => "metadata", :messagettl => 650}).should == "https://mock-account.queue.localhost/queue?comp=metadata&messagettl=650"
  end
  
  it "should include additional parameters when given althought when there is no comp" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    service.generate_request_uri("/queue", {:item => "%"}).should == "https://mock-account.queue.localhost/queue?item=%25"
  end
  
  it "should canonicalize headers (order lexicographical, trim values, and join by NEW_LINES)" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    headers = { "Content-Type" => "application/xml",
                "x-ms-prop-z" => "p",
                "x-ms-meta-name" => "a ",
                "x-other" => "other"}

    service.canonicalize_headers(headers).should == "x-ms-meta-name:a\nx-ms-prop-z:p"
  end
  
  it "should return empty string when no MS headers" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    headers = { "Content-Type" => "application/xml",
                "x-other" => "other"}

    service.canonicalize_headers(headers).should == ""
  end
  
  it "should cannonicalize message by appending account_name to the request path" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    service.canonicalize_message("http://localhost/queue?comp=list").should == "/mock-account/queue?comp=list"
  end
  
  it "should ignore every other querystring parameter rather than comp=" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    service.canonicalize_message("http://localhost/queue?myparam=1").should == "/mock-account/queue"
  end
  
  it "should properly canonicalize message when no parameter associated with it" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    service.canonicalize_message("http://mock-account.queue.core.windows.net/").should == "/mock-account/"
  end
  
  it "should properly canonicalize message when a resource is associated with it" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    service.canonicalize_message("http://mock-account.queue.core.windows.net/resource?comp=list").should == "/mock-account/resource?comp=list"
  end
  
  it "should generate request with proper headers" do
    mock_time = Time.new
    Time.stubs(:new).returns(mock_time)
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    
    expected_request_hash = {:headers => {'x-ms-Date' => mock_time.httpdate, "Content-Length" => "payload".length},
                             :method => :put, 
                             :url => "http://localhost/johnny",
                             :payload  => "payload"}
    
    # mock the generate signature method since we want to assert against a know value 
    service.expects(:generate_signature).with(expected_request_hash).returns("a_mock_signature")
    
    request = service.generate_request("PUT", "http://localhost/johnny", nil, "payload")
    request.headers["x-ms-Date"].should == mock_time.httpdate
    request.headers["Content-Length"].should == "payload".length
    request.headers["Authorization"] = "SharedKey mock-account:a_mock_signature"
  end
  
  it "should set content length when it is not provided" do
    mock_time = Time.new
    Time.stubs(:new).returns(mock_time)
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    
    expected_request_hash = {:headers => {'x-ms-Date' => mock_time.httpdate, 'Content-Length' => 0},
                             :method => :put, 
                             :url => "http://localhost/johnny",
                             :payload => nil}
    
    # mock the generate signature method since we want to assert against a know value 
    service.expects(:generate_signature).with(expected_request_hash).returns("a_mock_signature")
    
    request = service.generate_request("PUT", "http://localhost/johnny", nil)
    request.headers["x-ms-Date"].should == mock_time.httpdate
    request.headers["Content-Length"].should == 0
    request.headers["Authorization"] = "SharedKey mock-account:a_mock_signature"
  end
  
  it "should name headers properly when they are provided as symbols" do
     mock_time = Time.new
      Time.stubs(:new).returns(mock_time)
      service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")

      expected_request_hash = {:headers => {'x-ms-Date' => mock_time.httpdate, "Content-Length" => 0, 'Content-Type' => "plain/xml"},
                               :method => :put, 
                               :url => "http://localhost/johnny",
                               :payload  => nil}

      # mock the generate signature method since we want to assert against a know value 
      service.expects(:generate_signature).with(expected_request_hash).returns("a_mock_signature")
    
    request = service.generate_request("PUT", "http://localhost/johnny", {:Content_Type => "plain/xml"})
    request.headers["x-ms-Date"].should == mock_time.httpdate
    request.headers["Content-Length"].should == 0
    request.headers["Authorization"] = "SharedKey mock-account:a_mock_signature"
  end
  
  it "should cannonicalize message by appending account_name to the request path following 2009-09-19 version of the API" do
    service = WAZ::Queues::Service.new(:account_name => "myaccount", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    canonical_message = service.canonicalize_message20090919("http://myaccount.blob.core.windows.net/mycontainer?restype=container&comp=metadata")
    canonical_message.should == "/myaccount/mycontainer\ncomp:metadata\nrestype:container"
  end
end