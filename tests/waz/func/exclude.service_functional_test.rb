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
    options = { :account_name => "cherry", 
                :access_key => "J9NfKPlRQLXlDnVWWk2yGOBvKmn6QR65aGVgCM75ZhegkH9ff+x9B62xHn5IE2pOIYS8i7yvL1pPAfRVlFYvQA==" }

    WAZ::Storage::Base.establish_connection!(options)

    # excepts that the metadata for the queue changes queue behaves with put
    # it performs a validation whether metadata changed or not (if changed HTTP 409 conflict)
    queue = WAZ::Queues::Queue.ensure('my-queue-new')
    queue.clear
    queue.enqueue!("Hello, world!")
    messages = queue.lock()
    p messages.message_text
  end
end