$:.unshift(File.dirname(__FILE__))
require 'waz-storage'
# Application Files (massive include)
app_files = File.expand_path(File.join(File.dirname(__FILE__), 'waz', 'tables', '*.rb'))
Dir[app_files].each(&method(:load))