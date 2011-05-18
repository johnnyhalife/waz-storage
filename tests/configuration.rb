require 'rubygems'
%w{rspec rspec/autorun mocha restclient time hmac-sha2 base64}.each(&method(:require))

RSpec.configure do |config|
  config.mock_with :mocha
end
