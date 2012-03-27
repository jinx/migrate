require File.dirname(__FILE__) + '/family'

module Family
  # The specification for the family example.
  describe Parent do
    before(:all) do
      # Migrate the input.
      @migrated = Jinx::Migrator.new(
        :debug => true,
        :target => Parent,
        :mapping => File.expand_path('fields.yaml', File.dirname(__FILE__)),
        :defaults => File.expand_path('defaults.yaml', File.dirname(__FILE__)),
        :filters => File.expand_path('values.yaml', File.dirname(__FILE__)),
        :shims => File.expand_path('family.rb', File.dirname(__FILE__)),
        :input => File.expand_path('parents.csv', File.dirname(__FILE__))
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
