module Domain
  shims Parent
  
  class Parent
    # Make each Parent unique.
    include Jinx::Unique
  end
end
