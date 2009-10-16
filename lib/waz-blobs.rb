require 'time'
require 'cgi'
require 'base64'
require 'rexml/document'
require 'rexml/xpath'
require 'restclient'
require 'hmac-sha2'

$:.unshift(File.dirname(__FILE__))
require 'waz-storage'
require 'waz/blobs/blob_object'
require 'waz/blobs/container'
require 'waz/blobs/service'


