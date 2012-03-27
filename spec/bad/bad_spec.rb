require File.dirname(__FILE__) + '/../spec_helper'

module Domain
  RESULTS = File.dirname(__FILE__) + '/../../test/results'

  describe 'Bad' do
    # The rejects file.
    bad = RESULTS + '/bad/rejects.csv'
    
    # Migrate the input.
    migrated = Jinx::Migrator.new(:debug => true, :target => Parent, :bad => bad,
      :mapping => File.expand_path('fields.yaml', File.dirname(__FILE__)),
      :shims => File.expand_path('shims.rb', File.dirname(__FILE__)),
      :input => File.expand_path('parents.csv', File.dirname(__FILE__))
    ).to_a
    
    # Validate the migration.
    it "should migrate one record" do
      migrated.size.should be 1
    end
    it "should capture two bad records" do
      File.open(bad).to_a.size.should be 2
    end
  end
end
