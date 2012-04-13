module Model
  shims Parent
  
  class Parent
    @@id = 1
    
    def extract(io)
      io << [name, @@id]
      @@id += 1
    end
  end
end
  