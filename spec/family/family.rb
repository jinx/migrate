require File.dirname(__FILE__) + '/../spec_helper'

# Load the jinx family example.
require Bundler.environment.specs.detect { |s| s.name == 'jinx' }.full_gem_path + '/examples/family/lib/family'

module Family
  include Jinx::Migratable
  
  ROOT = File.dirname(__FILE__) + '/../../examples/family'
  DATA = ROOT + '/data'
  CONFIGS = ROOT + '/conf'
  SHIMS = ROOT + '/lib/shims.rb'
end
