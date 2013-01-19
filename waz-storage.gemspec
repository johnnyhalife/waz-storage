# -*- encoding: utf-8 -*-
Gem::Specification.new do |gem|
  gem.authors       = ['Johnny G. Halife']
  gem.email         = ['johnny.halife@me.com']
  gem.description   = %q{A simple implementation of Windows Azure Storage API for Ruby, inspired by the S3 gems and self experience of dealing with queues. The major goal of the whole gem is to enable ruby developers [like me =)] to leverage Windows Azure Storage features and have another option for cloud storage.}
  gem.summary       = %q{Client library for Windows Azure's Storage Service REST API}
  gem.homepage      = 'http://waz-storage.heroku.com'

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "waz-storage"
  gem.require_paths = ["lib"]
  gem.version       = "1.3.0"
  
  gem.test_files    = Dir['tests/**/*']

  gem.has_rdoc      = true
  gem.rdoc_options  << '--line-numbers' << '--inline-source' << '-A cattr_accessor=object'
  
  gem.add_dependency 'rest-client'
  gem.add_dependency 'ruby-hmac'
  
  gem.add_development_dependency 'rake'
  gem.add_development_dependency 'rspec'
  gem.add_development_dependency 'rdoc'
  gem.add_development_dependency 'simplecov'
  gem.add_development_dependency 'mocha'
end
