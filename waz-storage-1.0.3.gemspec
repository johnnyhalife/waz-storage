# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{waz-storage}
  s.version = "1.0.3"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Johnny G. Halife"]
  s.date = %q{2010-12-18}
  s.description = %q{A simple implementation of Windows Azure Storage API for Ruby, inspired by the S3 gems and self experience of dealing with queues. The major goal of the whole gem is to enable ruby developers [like me =)] to leverage Windows Azure Storage features and have another option for cloud storage.}
  s.email = %q{johnny.halife@me.com}
  s.files = ["rakefile", "lib/waz/blobs/blob_object.rb", "lib/waz/blobs/container.rb", "lib/waz/blobs/exceptions.rb", "lib/waz/blobs/service.rb", "lib/waz/queues/exceptions.rb", "lib/waz/queues/message.rb", "lib/waz/queues/queue.rb", "lib/waz/queues/service.rb", "lib/waz/storage/base.rb", "lib/waz/storage/core_service.rb", "lib/waz/storage/exceptions.rb", "lib/waz/storage/validation_rules.rb", "lib/waz/storage/version.rb", "lib/waz/tables/edm_type_helper.rb", "lib/waz/tables/exceptions.rb", "lib/waz/tables/service.rb", "lib/waz/tables/table.rb", "lib/waz/tables/table_array.rb", "lib/waz-blobs.rb", "lib/waz-queues.rb", "lib/waz-storage.rb", "lib/waz-tables.rb"]
  s.homepage = %q{http://waz-storage.heroku.com}
  s.rdoc_options = ["--line-numbers", "--inline-source", "-A cattr_accessor=object"]
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.7}
  s.summary = %q{Client library for Windows Azure's Storage Service REST API}

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<rest-client>, [">= 0"])
      s.add_runtime_dependency(%q<ruby-hmac>, [">= 0"])
    else
      s.add_dependency(%q<rest-client>, [">= 0"])
      s.add_dependency(%q<ruby-hmac>, [">= 0"])
    end
  else
    s.add_dependency(%q<rest-client>, [">= 0"])
    s.add_dependency(%q<ruby-hmac>, [">= 0"])
  end
end
