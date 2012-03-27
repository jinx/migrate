require File.dirname(__FILE__) + '/../../spec_helper'
require 'fileutils'
require 'jinx/csv/csvio'

describe Jinx::CsvIO do
  describe 'join' do
    SOURCE = File.expand_path('source.csv', File.dirname(__FILE__))
    
    TARGET = File.expand_path('target.csv', File.dirname(__FILE__))

    RESULTS = File.dirname(__FILE__) + '/../../../test/results/csv'

    OUTPUT = File.expand_path('output.csv', RESULTS)  

    before(:all) do
      FileUtils.rm_rf RESULTS
      Jinx::CsvIO.join(SOURCE, TARGET, OUTPUT)
      if File.exists?(OUTPUT) then
        @output = File.readlines(OUTPUT).map do |line|
          line.chomp.split(',').map { |s| s unless s.blank? }
        end
      end
    end
    
    it 'writes the output CSV file' do
      @output.should_not be nil
    end
    
    it 'joins each record' do
      @output.size.should be 10
    end
    
    it 'writes the output header row' do
      @output.first.should == ['A', 'B', 'U', 'X']
    end
    
    it 'writes the matching source and target' do
      @output[1].should == ['a1', 'b1', 'u', 'x']
      @output[2].should == ['a1', 'b1', 'v', 'x']
      @output[3].should == ['a1', 'b2', 'u', 'x']
      @output[4].should == ['a1', 'b2', 'u', 'y']
      @output[5].should == ['a2', 'b3', 'u', 'x']
    end
    
    it 'writes the unmatched source' do
      # Note that String split truncates the trailing blank array items,
      # so the comparison is to ['a2', 'b4', 'u'] rather than ['a2', 'b4', 'u', nil].
      @output[6].should == ['a2', 'b4', 'u']
      @output[9].should == ['a4', 'b7', 'u']
    end
    
    it 'writes the unmatched target' do
      @output[7].should == ['a2', 'b5', nil, 'x']
      @output[8].should == ['a3', nil, nil, 'x']
    end
  end
end
