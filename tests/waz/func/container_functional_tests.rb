# enabling the load of files from root (on RSpec)
$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../')

require 'rubygems'
require 'spec'
require 'mocha'
require 'waz-blobs'

describe "blobs service behavior" do
   
  it "should satisfy my expectations" do
     options = { :account_name => "your_account", 
                 :access_key => "your_key" }

    WAZ::Storage::Base.establish_connection!(options)
    
    container = WAZ::Blobs::Container.find('momo-container')
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
  end
end