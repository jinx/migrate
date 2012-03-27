# Load the jinx model example.

require Bundler.environment.specs.detect { |s| s.name == 'jinx' }.full_gem_path + '/test/helpers/model'

# Make the test domain classes migratable
module Domain
  include Jinx::Migratable
end
