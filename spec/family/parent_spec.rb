require File.dirname(__FILE__) + '/family'

module Family
  # The specification for the family example.
  describe Parent do
    before(:all) do
      # Migrate the input.
      @migrated = Jinx::Migrator.new(
        :debug => true,
        :target => Parent,
        :mapping => CONFIGS + '/parents/fields.yaml',
        :defaults => CONFIGS + '/parents/defaults.yaml',
        :filters => CONFIGS + '/parents/values.yaml',
        :shims => SHIMS,
        :input => DATA + '/parents.csv'
      ).to_a
    end
    
    # Validate the migration.
    it "should migrate the records" do
      @migrated.size.should be 2
    end
    
    it "should create a household" do
      @migrated.each do |parent|
        parent.household.should_not be nil
      end
    end
    
    it "should migrate the addresses" do
      @migrated.each do |parent|
        parent.household.address.should_not be nil
      end
    end
    
    it "should abbreviate the street" do
      addr = @migrated.first.household.address
      addr.street1.should match /St/
      addr.street1.should_not match /Street/
    end
    
    it "should add the default state" do
      @migrated.each do |parent|
        parent.household.address.state.should == 'IL'
      end
    end
    
    it "should migrate the spouse" do
      @migrated.first.spouse.should_not be nil
    end
    
    it "should set the spouse household" do
      hh = @migrated.first.household
      @migrated.first.spouse.household.should be hh
    end
  end
end
