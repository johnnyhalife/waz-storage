require 'rubygems'
%w{spec mocha restclient time hmac-sha2 base64}.each(&method(:require))

Spec::Runner.configure do |config|
  config.mock_with :mocha
end