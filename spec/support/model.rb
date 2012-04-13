# Load the jinx model example.
require Bundler.environment.specs.detect { |s| s.name == 'jinx' }.full_gem_path + '/spec/support/model'

# Make the test domain classes migratable
module Model
  include Jinx::Migratable
end
