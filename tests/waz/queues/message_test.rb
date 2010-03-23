# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')
require 'tests/configuration'
require 'lib/waz-queues'

describe "Message object behavior" do
  it "should delete message" do
    WAZ::Storage::Base.stubs(:default_connection).returns({:account_name => "my-account", :access_key => "key"})
    WAZ::Queues::Service.any_instance.expects(:delete_message).with("queue-10", "message id", "receipt")
    options = {:message_id => "message id", :message_text => "text", :expiration_time => Time.new, 
               :insertion_time => Time.new, :pop_receipt => "receipt", :queue_name => "queue-10"}

    message = WAZ::Queues::Message.new(options)
    message.destroy!
  end
  
  it "should throw when trying to delete a message peeked (no pop_receipt)" do
    options = {:message_id => "message id", :message_text => "text", :expiration_time => Time.new, 
               :insertion_time => Time.new, :queue_name => "queue-10" }

    message = WAZ::Queues::Message.new(options)

    lambda { message.destroy! }.should raise_error(WAZ::Queues::InvalidOperation)
  end
  
  it "should respond to dequeue_count property" do
    options = {:message_id => "message id", :message_text => "text", :expiration_time => Time.new, 
               :insertion_time => Time.new, :pop_receipt => "receipt", :queue_name => "queue-10", :dequeue_count => 1}

    message = WAZ::Queues::Message.new(options)
    message.dequeue_count.should == 1
  end
end