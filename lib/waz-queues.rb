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
require 'waz/queues/exceptions'
require 'waz/queues/message'
require 'waz/queues/queue'
require 'waz/queues/service'
require 'waz/queues/version'


