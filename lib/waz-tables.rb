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


