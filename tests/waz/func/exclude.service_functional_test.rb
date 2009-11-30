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
    options = { :account_name => "wazstoragejohnny", 
                :access_key => "Tm870FVNS14aNW1zsn13fZykc4yDKz82W8m4qujIZTayOJvhOePsjSFIsFnQF8rPnDaRJQJwzhoziI7ZtIWTsQ==" }

    WAZ::Storage::Base.establish_connection!(options)

    # excepts that the metadata for the queue changes queue behaves with put
    # it performs a validation whether metadata changed or not (if changed HTTP 409 conflict)
    queue ||= WAZ::Queues::Queue.find('testing-queue')
    queue = WAZ::Queues::Queue.create('testing-queue')
    queue.nil?.should == false
    
    queue.clear()
    queue.size.should == 0    

    1.times do |m|
      # enqueue a receives string. Message content can be anything up to 8KB
      # you can serialize and send anything that serializes to UTF-8 string
      queue.enqueue!("message##{m}")
    end
    
    queue.size.should == 1
    
    while(queue.size > 0) do
      # Since WAZ implements the peek lock pattern we are locking messages (not dequeuing)
      message = queue.lock
      message.dequeue_count.should == 1
      message.destroy!
    end
    
    queue.size.should == 0
  end
end