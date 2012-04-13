require File.dirname(__FILE__) + '/family'

module Family
  describe Child do
    before(:all) do
      # Migrate the input.
      @migrated = Jinx::Migrator.new(
        :debug => true,
        :target => Child,
        :mapping => CONFIGS + '/children/fields.yaml',
        :shims => SHIMS,
        :input => DATA + '/children.csv'
      ).to_a
    end
    
    # Validate the migration.
    it "should migrate the records" do
      @migrated.size.should be 3
    end
    
    it "should migrate the parents" do
      @migrated.each do |child|
        child.parents.size.should be 1
      end
    end
  end
end
