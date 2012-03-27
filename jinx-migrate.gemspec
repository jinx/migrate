require File.dirname(__FILE__) + '/lib/jinx/migration/version'
require 'date'

Gem::Specification.new do |s|
  s.name          = 'jinx-migrate'
  s.summary       = 'Jinx JSON plug-in.'
  s.description   = s.summary + '. See github.com/jinx/migrate for more information.'
  s.version       = Jinx::Migrate::VERSION
  s.date          = Date.today
  s.author        = 'OHSU'
  s.email         = "jinx.ruby@gmail.com"
  s.homepage      = 'http://github.com/jinx/migrate'
  s.require_path  = 'lib'
  s.bindir        = 'bin'
  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files spec`.split("\n")
  s.executables   = `git ls-files bin`.split("\n").map{ |f| File.basename(f) }
  s.add_runtime_dependency     'rack'
  s.add_runtime_dependency     'bundler'
  s.add_runtime_dependency     'fastercsv'
  s.add_development_dependency 'rake'
  s.add_development_dependency 'rspec', '~> 1.3.2'
  s.has_rdoc      = 'yard'
  s.license       = 'MIT'
  s.rubyforge_project = 'jinx'
end
