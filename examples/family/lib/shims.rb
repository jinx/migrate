module Family
  # Declares the classes modified for migration.
  shims Parent

  class Parent
    # Augments the migration by setting the spouse household.
    #
    # @param [{Symbol => Object}] row the input row field => value hash
    # @param [<Resource>] migrated the migrated instances
    def migrate(row, migrated)
      super
      if spouse then
        spouse.household = migrated.detect { |m| Household === m }
      end
    end
  end
end