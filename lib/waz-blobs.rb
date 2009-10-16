require 'time'
require 'cgi'
require 'base64'
require 'rexml/document'
require 'rexml/xpath'
require 'restclient'
require 'hmac-sha2'

$:.unshift(File.dirname(__FILE__))
require 'waz/storage/base'
require 'waz/storage/core_service'
require 'waz/storage/exceptions'
require 'waz/storage/version'
require 'waz/blobs/blob_object'
require 'waz/blobs/container'
require 'waz/blobs/exceptions'
require 'waz/blobs/service'
require 'waz/blobs/version'


