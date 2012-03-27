require File.dirname(__FILE__) + '/../spec_helper'

module Domain
  describe 'Unique' do
    # Add the parent metadata definition.
    Domain.definitions File.dirname(__FILE__)
    
    # Migrate the input.
    migrated = Jinx::Migrator.new(:debug => true, :target => Parent, :unique => true,
      :shims => File.expand_path('shims.rb', File.dirname(__FILE__)),
      :mapping => File.expand_path('fields.yaml', File.dirname(__FILE__)),
      :input => File.expand_path('parents.csv', File.dirname(__FILE__))
    ).to_a
    
    # Validate the migration.
    it "should make the secondary key unique" do
      migrated.each { |p|p.name.should match /\w+_\d+/ }
    end
  end
end
