require File.dirname(__FILE__) + '/../spec_helper'

module Domain
  describe 'Filter' do
    before(:all) do
      @migrated = Jinx::Migrator.new(:debug => true, :target => Parent,
        :mapping => File.expand_path('fields.yaml', File.dirname(__FILE__)),
        :filters => File.expand_path('values.yaml', File.dirname(__FILE__)),
        :input => File.expand_path('parents.csv', File.dirname(__FILE__))
      ).to_a
    end
    
    it "should filter the name" do
      @migrated.size.should be 3
      @migrated[0].name.should == 'Joseph'
      @migrated[1].name.should == 'Christine'
      @migrated[2].name.should == 'Other'
    end
  end
end
