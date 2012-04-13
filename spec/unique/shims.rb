require 'jinx/resource/unique'

module Model
  shims Parent
  
  class Parent
    # Make each Parent unique.
    include Jinx::Unique
  end
end
