require 'simplecov'
SimpleCov.start do
  add_group "Gem files" do |src_file|
    lib_path = File.expand_path("../lib", File.dirname(__FILE__))
    src_file.filename.start_with?(lib_path)
  end
end

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
