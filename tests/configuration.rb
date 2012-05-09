require 'rubygems'
require 'rspec'
require 'rspec/autorun'
require 'mocha'
require 'restclient'
require 'time'
require 'hmac-sha2'
require 'base64'
require 'rexml/document'
require 'rexml/xpath'

RSpec.configure do |config|
  config.mock_with :mocha
end
