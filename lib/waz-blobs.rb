$:.unshift(File.dirname(__FILE__))
require 'waz-storage'
# Application Files (massive include)
app_files = File.expand_path(File.join('lib', 'waz', 'blobs', '*.rb'))
Dir[app_files].each(&method(:load))