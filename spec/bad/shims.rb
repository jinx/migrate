module Domain
  shims Parent
  
  class Parent
    # Simulate an error.
    def migrate_name(value, row)
      raise StandardError.new("Simulated error") if value == 'Mark'
      value
    end
    
    # Simulate invalidation.
    def migration_valid?
      name == 'Tom'
    end
  end
end