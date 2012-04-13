require File.dirname(__FILE__) + '/../spec_helper'

module Model
  describe 'Extract' do
    EXTRACT = File.expand_path('ids.csv', Migration::Test::RESULTS + '/extract')
    
    HEADERS = ['Name', 'Id']
    
    before(:all) do
      # Migrate the input.
      @migrated = Jinx::Migrator.new(
        :debug => true,
        :target => Parent,
        :mapping => File.expand_path('fields.yaml', File.dirname(__FILE__)),
        :extract => EXTRACT,
        :extract_headers => HEADERS,
        :shims => File.expand_path('extract.rb', File.dirname(__FILE__)),
        :input => File.expand_path('parents.csv', File.dirname(__FILE__))
      ).to_a
    end
    
    it "should migrate the records" do
      @migrated.size.should be 3
    end
    
    it "should create the extract" do
      xtr = File.readlines(EXTRACT).map { |line| line.chomp }
      xtr.size.should be 4
      xtr[0].should == HEADERS.join(',')
      1.upto(3) { |i| xtr[i].chomp.split(',').should == [@migrated[i - 1].name, i.to_s] }
    end
  end
end
