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

describe "queues service behavior" do   
  it "should satisfy my expectations" do
    service = WAZ::Storage::Base.establish_connection!(:account_name => "copaworkshop", 
                                                       :access_key => "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")

    # excepts that the metadata for the queue changes queue behaves with put
    # it performs a validation whether metadata changed or not (if changed HTTP 409 conflict)
    queue = WAZ::Queues::Queue.create('testing-queue')
    queue.nil?.should == false
    
    queue.clear()
    queue.size.should == 0    
    
    10.times do |m|
      # enqueue a receives string. Message content can be anything up to 8KB
      # you can serialize and send anything that serializes to UTF-8 string
      queue.enqueue!("message##{m}")
    end
    
    queue.size.should == 10
    
    while(queue.size > 0) do
      # Since WAZ implements the peek lock pattern we are locking messages (not dequeuing)
      messages = queue.lock(10)
      messages.size.should == 10
      
      puts "dequeued message: #{messages.size}"
      messages.each {|m| m.destroy!}
    end
    
    queue.size.should == 0
  end
end