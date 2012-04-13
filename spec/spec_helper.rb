require 'rubygems'
require 'bundler/setup'
Bundler.require(:test, :development)

require 'jinx/migration/migrator'

# Open the logger.
Jinx::Log.instance.open(File.dirname(__FILE__) + '/../test/results/log/jinx.log', :debug => true)

module Migration
  module Test
    RESULTS = File.dirname(__FILE__) + '/../test/results'
  end
end

# Add the support files.
Dir.glob(File.dirname(__FILE__) + '/support/**/*.rb').each { |f| require f }
