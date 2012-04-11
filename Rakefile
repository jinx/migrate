require File.dirname(__FILE__) + '/lib/jinx/migration/version'

# the gem name
GEM = 'jinx-migrate'
GEM_VERSION = Jinx::Migrate::VERSION

WINDOWS = (Config::CONFIG['host_os'] =~ /mingw|win32|cygwin/ ? true : false) rescue false
SUDO = WINDOWS ? '' : 'sudo'

desc 'Default: run the specs'
task :default => :spec

desc "Builds the gem"
task :gem do
  sh "jgem build #{GEM}.gemspec"
end

desc "Installs the gem"
task :install => :gem do
  sh "#{SUDO} jgem install #{GEM}-#{GEM_VERSION}.gem"
end

desc 'Documents the API'
task :doc do
  FileUtils.rm_rf 'doc/api'
  sh 'yardoc'
end

desc 'Runs the spec tests'
task :spec do
  Dir['spec/**/*_spec.rb'].each { |f| sh "rspec #{f}" rescue nil }
end

desc 'Runs the unit tests'
task :unit do
  Dir['test/**/*_test.rb'].each { |f| sh "jruby #{f}" rescue nil }
end

desc 'Runs all tests'
task :test => [:spec, :unit]
