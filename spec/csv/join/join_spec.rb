require 'spec/csv/join/join_helper'

shared_examples 'a join for all source fields' do 
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

describe 'Join' do
  include Jinx::JoinHelper
  
  context 'Join for all source fields' do
    before(:all) { @output = join(:source, :target) }
    
    it_behaves_like 'a join for all source fields'
  end
  
  context 'Join with block' do
    before(:all) do
      @output = join(:source, :target) do |rec|
        curr = rec[0..1]
        if curr == @prev then
          rec[1] = nil
        else 
          @prev = curr
        end
        rec unless curr == ['a2', 'b3']
      end
    end

    it 'preserves the output header row' do
      @output.first.should == ['A', 'B', 'U', 'X']
    end

    it 'applies the block to the records before writing them to the ouput' do
      @output[1].should == ['a1', 'b1', 'u', 'x']
      @output[2].should == ['a1', nil, 'v', 'x']
      @output[3].should == ['a1', 'b2', 'u', 'x']
      @output[4].should == ['a1', nil, 'u', 'y']
    end

    it 'omits the record if the block returns nil' do
      @output[5].should == ['a2', 'b4', 'u']
    end
  end
  
  context 'Join for jumbled source and target fields' do
    before(:all) { @output = join(:jumbled_src, :jumbled_tgt) }
    
    it_behaves_like 'a join for all source fields'
  end
  
  context 'Join for only the key source fields' do
    before(:all) { @output = join(:source, :target, 'A', 'B') }

    it 'joins each record' do
      @output.size.should be 10
    end

    it 'writes the output header row' do
      @output.first.should == ['A', 'B', 'X']
    end

    it 'writes the matching source and target records without the source-specific fields' do
      @output[1].should == ['a1', 'b1', 'x']
      @output[2].should == ['a1', 'b1', 'x']
      @output[3].should == ['a1', 'b2', 'x']
      @output[4].should == ['a1', 'b2', 'y']
      @output[5].should == ['a2', 'b3', 'x']
      @output[6].should == ['a2', 'b4']
      @output[7].should == ['a2', 'b5', 'x']
      @output[8].should == ['a3', nil, 'x']
      @output[9].should == ['a4', 'b7']
    end
  end
end
