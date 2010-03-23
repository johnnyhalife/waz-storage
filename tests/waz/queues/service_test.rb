# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')
require 'tests/configuration'
require 'lib/waz-queues'

describe "Windows Azure Queues API service" do 
  it "should list queues" do
    # setup mocks
    expected_url = "http://my_account.queue.core.windows.net/?comp=list"
    expected_response = <<-eos
                      <?xml version="1.0" encoding="utf-8"?>
                      <EnumerationResults AccountName="http://myaccount.queue.core.windows.net">
                        <Queues>
                          <Queue>
                            <Name>mock-queue</Name>
                            <Url>http://myaccount.queue.core.windows.net/mock-queue</Url>
                          </Queue>
                        </Queues>
                      </EnumerationResults>
                      eos

    mock_request = RestClient::Request.new(:method => :get, :url => expected_url)
    mock_request.expects(:execute).returns(expected_response)

    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    # setup mocha expectations
    service.expects(:generate_request_uri).with(nil, :comp => 'list').returns(expected_url)
    service.expects(:generate_request).with(:get, expected_url, {:x_ms_version => '2009-09-19'}, nil).returns(mock_request)

    queues = service.list_queues()
    queues.first()[:name].should == "mock-queue"
    queues.first()[:url].should == "http://myaccount.queue.core.windows.net/mock-queue"
  end
  
  it "should create queue" do
    expected_url = "http://myaccount.queue.core.windows.net/mock-queue"
    mock_request = RestClient::Request.new(:method => :get, :url => expected_url)
    mock_request.stubs(:execute)
    
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    # setup mocha expectations
    service.expects(:generate_request_uri).with("mock-queue", nil).returns(expected_url)
    service.expects(:generate_request).with(:put, expected_url, {:x_ms_meta_priority => "high-importance", :x_ms_version => '2009-09-19'}, nil).returns(mock_request)

    service.create_queue("mock-queue", {:x_ms_meta_priority => "high-importance"})
  end
  
  it "should delete queue" do
    expected_url = "http://myaccount.queue.core.windows.net/mock-queue"
    mock_response = mock()
    mock_request = RestClient::Request.new(:method => :get, :url => expected_url)
    mock_request.stubs(:execute).returns(mock_response)

    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    # setup mocha expectations
    service.expects(:generate_request_uri).with("mock-queue", {}).returns(expected_url)
    service.expects(:generate_request).with(:delete, expected_url, {:x_ms_version => '2009-09-19'}, nil).returns(mock_request)
    service.delete_queue('mock-queue')
  end
  
  it "should get queue metadata" do
    expected_url = "http://myaccount.queue.core.windows.net/mock-queue?comp=metadata"
    mock_response = mock()
    mock_response.stubs(:headers).returns({ :x_ms_request_id => "fake-id" })
    mock_request = RestClient::Request.new(:method => :head, :url => expected_url)
    mock_request.stubs(:execute).returns(mock_response)
    
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    # setup mocha expectations
    service.expects(:generate_request_uri).with("mock-queue", {:comp => 'metadata'}).returns(expected_url)
    service.expects(:generate_request).with(:head, expected_url, {:x_ms_version => '2009-09-19'}, nil).returns(mock_request)
    service.get_queue_metadata('mock-queue').should == mock_response.headers
  end
  
  it "should set queue metadata" do
    expected_url = "http://myaccount.queue.core.windows.net/mock-queue"
    mock_request = RestClient::Request.new(:method => :get, :url => expected_url)
    mock_request.stubs(:execute)

    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    # setup mocha expectations
    service.expects(:generate_request_uri).with("mock-queue", :comp => 'metadata').returns(expected_url)
    service.expects(:generate_request).with(:put, expected_url, {:x_ms_version => '2009-09-19', :x_ms_meta_priority => "high-importance"}, nil).returns(mock_request)

    service.set_queue_metadata("mock-queue", {:x_ms_meta_priority => "high-importance"})
  end
  
  it "should enqueue message" do
    expected_url = "http://myaccount.queue.core.windows.net/mock-queue/messages?messagettl=604800"
    mock_request = RestClient::Request.new(:method => :post, :url => expected_url)
    mock_request.stubs(:execute)
    
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    # setup mocha expectations
    payload = "<?xml version=\"1.0\" encoding=\"utf-8\"?><QueueMessage><MessageText>this is the message payload</MessageText></QueueMessage>"                       
    service.expects(:generate_request_uri).with("mock-queue/messages", { :messagettl => 604800 }).returns(expected_url)
    service.expects(:generate_request).with(:post, expected_url, { 'Content-Type' => "application/xml", :x_ms_version => '2009-09-19'}, payload).returns(mock_request)

    service.enqueue("mock-queue", "this is the message payload")
  end
  
  it "should get messages from queue" do
    expected_url = "http://myaccount.queue.core.windows.net/mock-queue/messages"
    expected_response = <<-eos 
                        <?xml version="1.0" encoding="utf-8"?>
                          <QueueMessagesList>
                            <QueueMessage>
                              <MessageId>message_id</MessageId>
                              <InsertionTime>Mon, 22 Sep 2008 23:29:20 GMT</InsertionTime>
                              <ExpirationTime>Mon, 29 Sep 2008 23:29:20 GMT</ExpirationTime>
                              <PopReceipt>receipt</PopReceipt>
                              <TimeNextVisible>Tue, 23 Sep 2008 05:29:20 GMT</TimeNextVisible>
                              <MessageText>text</MessageText>
                            </QueueMessage>
                          </QueueMessagesList>
                        eos
                        
    mock_request = RestClient::Request.new(:method => :get, :url => expected_url)
    mock_request.stubs(:execute).returns(expected_response)
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    
    # setup mocha expectations
    service.expects(:generate_request_uri).with("mock-queue/messages", {}).returns(expected_url)
    service.expects(:generate_request).with(:get, expected_url, {:x_ms_version => "2009-09-19"}, nil).returns(mock_request)

    messages = service.get_messages("mock-queue")
    messages.first()[:message_id].should == "message_id"
    messages.first()[:pop_receipt].should == "receipt"
    messages.first()[:message_text].should == "text"
    messages.first()[:insertion_time].nil?.should == false
    messages.first()[:expiration_time].nil?.should == false
    messages.first()[:time_next_visible].nil?.should == false
  end
  
  it "should get messages from queue specifiying additional parameters" do
    expected_url = "http://myaccount.queue.core.windows.net/mock-queue/messages"
    expected_response = <<-eos 
                        <?xml version="1.0" encoding="utf-8"?>
                          <QueueMessagesList>
                            <QueueMessage>
                              <MessageId>message_id</MessageId>
                              <InsertionTime>Mon, 22 Sep 2008 23:29:20 GMT</InsertionTime>
                              <ExpirationTime>Mon, 29 Sep 2008 23:29:20 GMT</ExpirationTime>
                              <PopReceipt>receipt</PopReceipt>
                              <TimeNextVisible>Tue, 23 Sep 2008 05:29:20 GMT</TimeNextVisible>
                              <MessageText>text</MessageText>
                            </QueueMessage>
                          </QueueMessagesList>
                        eos
                        
    mock_request = RestClient::Request.new(:method => :get, :url => expected_url)
    mock_request.stubs(:execute).returns(expected_response)
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    
    # setup mocha expectations
    service.expects(:generate_request_uri).with("mock-queue/messages", {:num_of_messages => 2, :visibility_timeout => 3}).returns(expected_url)
    service.expects(:generate_request).with(:get, expected_url, {:x_ms_version => "2009-09-19"}, nil).returns(mock_request)

    messages = service.get_messages("mock-queue", :num_of_messages => 2, :visibility_timeout => 3)
    messages.first()[:message_id].should == "message_id"
    messages.first()[:pop_receipt].should == "receipt"
    messages.first()[:message_text].should == "text"
    messages.first()[:insertion_time].nil?.should == false
    messages.first()[:expiration_time].nil?.should == false
    messages.first()[:time_next_visible].nil?.should == false
  end
  
  it "should raise exception when :num_of_messages is out of range (1 - 32)" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    lambda{ service.get_messages("mock-queue", :num_of_messages => 0) }.should raise_error(WAZ::Queues::OptionOutOfRange)
    lambda{ service.get_messages("mock-queue", :num_of_messages => 33) }.should raise_error(WAZ::Queues::OptionOutOfRange)    
  end
  
  it "should raise exception when :visibility_timeout is out of range (1 - 7200)" do
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    lambda{ service.get_messages("mock-queue", :visibility_timeout => 0) }.should raise_error(WAZ::Queues::OptionOutOfRange)
    lambda{ service.get_messages("mock-queue", :visibility_timeout => 7201) }.should raise_error(WAZ::Queues::OptionOutOfRange)    
  end
  
  it "should peek message from queue" do
    expected_url = "http://myaccount.queue.core.windows.net/mock-queue/messages"
    expected_response = <<-eos 
                        <?xml version="1.0" encoding="utf-8"?>
                          <QueueMessagesList>
                            <QueueMessage>
                              <MessageId>message_id</MessageId>
                              <InsertionTime>Mon, 22 Sep 2008 23:29:20 GMT</InsertionTime>
                              <ExpirationTime>Mon, 29 Sep 2008 23:29:20 GMT</ExpirationTime>
                              <MessageText>text</MessageText>
                              <DequeueCount>5</DequeueCount>
                            </QueueMessage>
                          </QueueMessagesList>
                        eos
                        
    mock_request = RestClient::Request.new(:method => :get, :url => expected_url)
    mock_request.stubs(:execute).returns(expected_response)
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    
    # setup mocha expectations
    service.expects(:generate_request_uri).with("mock-queue/messages", {:peek_only => true}).returns(expected_url)
    service.expects(:generate_request).with(:get, expected_url, {:x_ms_version => "2009-09-19"}, nil).returns(mock_request)

    messages = service.peek("mock-queue")
    messages.first()[:message_id].should == "message_id"
    messages.first()[:message_text].should == "text"
    messages.first()[:insertion_time].nil?.should == false
    messages.first()[:expiration_time].nil?.should == false
    messages.first()[:dequeue_count].should == 5
  end
  
  it "should peek messages from queue when :num_of_messages is specified" do
    expected_url = "http://myaccount.queue.core.windows.net/mock-queue/messages"
    expected_response = <<-eos 
                        <?xml version="1.0" encoding="utf-8"?>
                          <QueueMessagesList>
                            <QueueMessage>
                              <MessageId>message_id</MessageId>
                              <InsertionTime>Mon, 22 Sep 2008 23:29:20 GMT</InsertionTime>
                              <ExpirationTime>Mon, 29 Sep 2008 23:29:20 GMT</ExpirationTime>
                              <MessageText>text</MessageText>
                              <DequeueCount>5</DequeueCount>
                            </QueueMessage>
                          </QueueMessagesList>
                        eos
                        
    mock_request = RestClient::Request.new(:method => :get, :url => expected_url)
    mock_request.stubs(:execute).returns(expected_response)
    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    
    # setup mocha expectations
    service.expects(:generate_request_uri).with("mock-queue/messages", {:peek_only => true, :num_of_messages => 32}).returns(expected_url)
    service.expects(:generate_request).with(:get, expected_url, {:x_ms_version => "2009-09-19"}, nil).returns(mock_request)

    messages = service.peek("mock-queue", {:num_of_messages => 32})
    messages.first()[:message_id].should == "message_id"
    messages.first()[:message_text].should == "text"
    messages.first()[:insertion_time].nil?.should == false
    messages.first()[:expiration_time].nil?.should == false
    messages.first()[:dequeue_count].should == 5
  end
  
  it "should delete message" do
    expected_url = "http://myaccount.queue.core.windows.net/mock-queue/messages/message_id?popreceipt=pop_receipt"
    mock_request = RestClient::Request.new(:method => :get, :url => expected_url)
    mock_request.stubs(:execute)

    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    # setup mocha expectations
    service.expects(:generate_request_uri).with("mock-queue/messages/message_id", { :pop_receipt => "pop_receipt" }).returns(expected_url)
    service.expects(:generate_request).with(:delete, expected_url, {}, nil).returns(mock_request)

    service.delete_message("mock-queue", "message_id", "pop_receipt")
  end
  
  it "should clear queue" do
    expected_url = "http://myaccount.queue.core.windows.net/mock-queue/messages"
    mock_request = RestClient::Request.new(:method => :get, :url => expected_url)
    mock_request.stubs(:execute)

    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    # setup mocha expectations
    service.expects(:generate_request_uri).with("mock-queue/messages", {}).returns(expected_url)
    service.expects(:generate_request).with(:delete, expected_url, {:x_ms_version => '2009-09-19'}, nil).returns(mock_request)

    service.clear_queue("mock-queue")
  end
  
  it "should list queues with metadata" do
    # setup mocks
    expected_url = "http://my_account.queue.core.windows.net/?comp=list&include=metadata"
    expected_response = <<-eos
                      <?xml version="1.0" encoding="utf-8"?>
                      <EnumerationResults AccountName="http://myaccount.queue.core.windows.net">
                        <Queues>
                          <Queue>
                            <Name>mock-queue</Name>
                            <Url>http://myaccount.queue.core.windows.net/mock-queue</Url>
                            <Metadata>
                              <x-ms-name>Custom Queue</x-ms-name>
                            </Metadata>
                          </Queue>
                        </Queues>
                      </EnumerationResults>
                      eos

    mock_request = RestClient::Request.new(:method => :get, :url => expected_url)
    mock_request.expects(:execute).returns(expected_response)

    service = WAZ::Queues::Service.new(:account_name => "mock-account", :access_key => "mock-key", :type_of_service => "queue", :use_ssl => true, :base_url => "localhost")
    # setup mocha expectations
    service.expects(:generate_request_uri).with(nil, {:comp => 'list', :include => 'metadata'}).returns(expected_url)
    service.expects(:generate_request).with(:get, expected_url, {:x_ms_version => '2009-09-19'}, nil).returns(mock_request)

    queues = service.list_queues({:include => 'metadata'})
    queues.first()[:name].should == "mock-queue"
    queues.first()[:url].should == "http://myaccount.queue.core.windows.net/mock-queue"
    queues.first()[:metadata][:x_ms_name] = 'Custom Queue'
  end
end