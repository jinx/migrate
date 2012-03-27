require File.dirname(__FILE__) + '/../helper'

# Load the jinx clinical trials example.
require Bundler.environment.specs.detect { |s| s.name == 'jinx' }.full_gem_path + '/examples/clinical_trials/lib/clinical_trials'
