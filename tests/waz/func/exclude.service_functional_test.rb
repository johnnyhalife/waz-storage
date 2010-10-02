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


include WAZ::Blobs

describe "troubleshooting blob service behavior" do   
  it "should satisfy my expectations" do
  end
end