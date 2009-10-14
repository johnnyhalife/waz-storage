it "should create the appropiated request for listing containers" do
  service = WAZ::Blobs::Service.new("myaccount", "mykey")  
  #service.list_containers.should == "http://myaccount.blob.core.windows.net/?comp=list"
end 

it "should enable additional parameters for container listing" do
  service = WAZ::Blobs::Service.new("myaccount", "mykey")
  expected = "http://myaccount.blob.core.windows.net/?comp=list&maxresults=500&prefix=myContainer"
  #service.list_containers(:prefix => "myContainer", :max_results => 500).should == expected
end

it "should generate the appropiated request for list_containers" do
  custom_time = Time.new
  Time.any_instance.stubs(:new).returns(custom_time)
  service = WAZ::Blobs::Service.new("myaccount", "mykey")
  
  #request = service.list_containers
  #request.method.should == "GET"
  #request.headers["x-ms-Date"].should == custom_time.httpdate
  #request.headers["Authorization"].should =~ /^SharedKey myaccount:/
  #request.url.should == "http://myaccount.blob.core.windows.net/?comp=list"
  
end

it "should list containers on WAZ" do
  service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
  service.list_containers
end

it "should get container acl" do
    service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
    service.get_container_acl("downloads").should == true
    service.get_container_acl("my-container").should == false
end

it "should set container acl" do
    service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
    service.set_container_acl("my-container", true).should == 200
    service.get_container_acl("my-container").should == true
end

it "should return container properties" do
  service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
  service.container_properties("my-container")
end

it "should set container properties" do
  service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
  service.set_container_properties("my-container", {:x_ms_meta_Name => "Robberttina"}).should == 200
end

it "should create container" do
  service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
  service.create_container("container1")
end

it "should create container" do
  service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
  service.delete_container("my-container").should == 202
end

it "should list blobs" do
  service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
  service.list_blobs("downloads").should == 200
end

it "should put blob" do
  service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
  service.put_blob("downloads", "hello", "hello world dadas", "text/plain; charset=UTF-8").should == 201
end

it "should get blob" do
  service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
  service.get_blob("downloads/hello").should  == "hello world dadas"
end

it "should get blob metadata" do
  service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
  service.get_blob_properties("downloads/hello").should == 200
end

it "should set blob metadata" do
  service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
  service.set_blob_properties("downloads/hello", :x_ms_meta_custom => "customHeader").should == 200
end

it "should delete blob" do
  service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
  service.delete_blob("downloads/hello").should == 202
end

it "should copy blob" do
  service = WAZ::Blobs::Service.new("copaworkshop", "cEsGVWPxnYQFpwxpqjJEPC1aROCSGlLT9yQCZmGvdGz2s19ZXjso+mV56wAiT+g+JDuIWz8qWNkrpzXBtqCm7g==")
  service.copy_blob("downloads/hello", "downloads/hello-copy").should == 202
end