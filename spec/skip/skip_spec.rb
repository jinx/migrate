require File.dirname(__FILE__) + '/../spec_helper'

module Domain  
  describe 'Skip' do
    before(:all) do
      @migrated = Jinx::Migrator.new(:debug => true, :target => Parent,
        :mapping => File.expand_path('fields.yaml', File.dirname(__FILE__)),
        :from => 2,
        :input => File.expand_path('parents.csv', File.dirname(__FILE__))
      ).to_a
    end
    
    it "should skip the first record" do
      @migrated.size.should be 1
    end
  end
end
