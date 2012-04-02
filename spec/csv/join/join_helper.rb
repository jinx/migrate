require 'spec/spec_helper'
require 'fileutils'
require 'jinx/csv/csvio'

SOURCE = File.expand_path('source.csv', File.dirname(__FILE__))

TARGET = File.expand_path('target.csv', File.dirname(__FILE__))

RESULTS = File.dirname(__FILE__) + '/../../../test/results/join'

OUTPUT = File.expand_path('output.csv', RESULTS)

module Jinx
  module JoinHelper
    # Joins the given source fixture to the target fixture on the specified fields.
    #
    # @param [Symbol] source the source file fixture in the join spec directory
    # @param [Symbol] target the target file fixture in the join spec directory
    # @param [<String>] fields the source fields (default is all source fields)
    # @return [<<String>>] the output records
    def join(source, target, *fields, &block)
      FileUtils.rm_rf OUTPUT
      sf = File.expand_path("#{source}.csv", File.dirname(__FILE__))
      tf = File.expand_path("#{target}.csv", File.dirname(__FILE__))
      Jinx::CsvIO.join(sf, :to => tf, :for => fields, :as => OUTPUT, &block)
      if File.exists?(OUTPUT) then
        File.readlines(OUTPUT).map do |line|
          line.chomp.split(',').map { |s| s unless s.blank? }
        end
      else
        Array::EMPTY_ARRAY
      end
    end
  end
end
