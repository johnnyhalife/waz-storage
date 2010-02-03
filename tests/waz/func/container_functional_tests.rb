# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')

require 'rubygems'
require 'spec'
require 'mocha'
require 'lib/waz-blobs'

describe "blobs service behavior" do
   
  it "should satisfy my expectations" do
    options = { :account_name => "wazstoragejohnny", 
                :access_key => "Tm870FVNS14aNW1zsn13fZykc4yDKz82W8m4qujIZTayOJvhOePsjSFIsFnQF8rPnDaRJQJwzhoziI7ZtIWTsQ==" }

    WAZ::Storage::Base.establish_connection(options) do    
      container = (WAZ::Blobs::Container.find('momo-container') || WAZ::Blobs::Container.create('momo-container'))
      container.nil?.should == false
    
      container.put_properties!(:x_ms_meta_owner => "Ezequiel Morito")
      container.metadata[:x_ms_meta_owner].should == "Ezequiel Morito"

      container.public_access = true
      container.public_access?.should == true
    
      container.store("hello.txt", "Hola Don Julio Morito y Jedib!", "plain/text")

      blob = container["hello.txt"]
      blob.nil?.should == false
    
      blob.put_properties!(:x_ms_meta_owner => "Other owner")
      blob.metadata[:x_ms_meta_owner].should == "Other owner"
    
      blob.value.should == "Hola Don Julio Morito y Jedib!"
      
      container.blobs.each do |blob|
          puts "#{blob.path}<br/>"
      end
    
      WAZ::Blobs::Container.list.each do |container|
          puts "#{container.name}<br/>"
      end

      container = WAZ::Blobs::Container.find('copy-test')
      container ||= WAZ::Blobs::Container.create('copy-test')
      container.nil?.should == false
      container.store('blob', 'payload', 'plain/text')
      
      container.public_access = true
      container.public_access?.should == true
      
      blob = container['blob']
      blob.nil?.should == false
      
      copy = blob.copy('copy-test/blob-copy')
      copy.nil?.should == false
      
      copy.value.should == blob.value
      
      blob_copy = container['blob-copy']
      blob_copy.nil?.should == false
    end
    
    WAZ::Storage::Base.connected?.should == false
  end
end