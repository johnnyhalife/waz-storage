require 'time'
require 'cgi'
require 'base64'
require 'rexml/document'
require 'rexml/xpath'
require 'restclient'
require 'hmac-sha2'

$:.unshift(File.dirname(__FILE__))
require 'waz-storage'
require 'waz/tables/exceptions'
require 'waz/tables/table'
require 'waz/tables/service'
require 'waz/tables/edm_type_helper'

# extendes the Symbol class to assign a type to an entity field
class Symbol
  attr_accessor :edm_type
end
