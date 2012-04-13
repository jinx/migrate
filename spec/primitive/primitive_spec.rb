require File.dirname(__FILE__) + '/../spec_helper'

module Model
  describe 'Primitive' do
    before(:all) do
      # Migrate the input.
      @migrated = Jinx::Migrator.new(:debug => true, :target => Child,
        :mapping => File.expand_path('fields.yaml', File.dirname(__FILE__)),
        :input => File.expand_path('children.csv', File.dirname(__FILE__))
      ).to_a
    end
    
    it "should migrate the records" do
      @migrated.size.should be 3
    end
    
    it "should capture the primitive fields" do
      @migrated.first.name.should == 'Jane'
      @migrated.first.flag.should be true
      @migrated.first.cardinal.should be 1
      @migrated.first.decimal.should == 2.2
    end
  end
end
