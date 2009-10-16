require 'time'
require 'cgi'
require 'base64'
require 'rexml/document'
require 'rexml/xpath'

require 'restclient'
require 'hmac-sha2'

$:.unshift(File.dirname(__FILE__))
require 'waz-storage'
require 'waz/queues/exceptions'
require 'waz/queues/message'
require 'waz/queues/queue'
require 'waz/queues/service'
require 'waz/queues/version'


