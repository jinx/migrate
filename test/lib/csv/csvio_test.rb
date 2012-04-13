require 'rubygems'
require 'bundler/setup'
Bundler.require(:test, :development)

require 'jinx/migration/migrator'

# Open the logger.
Jinx::Log.instance.open(File.dirname(__FILE__) + '/../../results/log/jinx.log', :debug => true)

require 'test/unit'
require 'ftools'
require 'date'
require 'jinx/helpers/file_separator'
require 'jinx/csv/csvio'

class CsvIOTest < Test::Unit::TestCase
  TEST_DIR = File.dirname(__FILE__) + '/../..'
  FIXTURES_DIR = TEST_DIR + '/fixtures/csv/data'
  OUTPUT_DIR = TEST_DIR + '/results/csv'

  def setup
    File.makedirs(OUTPUT_DIR)
  end

  def test_read
    loader = Jinx::CsvIO.new(File.join(FIXTURES_DIR, 'variety.csv'))
    loader.each do |row|
      assert_not_nil(row[:id], "Missing id")
      assert_not_nil(row[:string_field], "Missing string_field")
      assert_not_nil(row[:integer], "Missing integer method")
      assert(Integer === row[:integer], "Incorrect integer field value #{row[:integer]} type")
      assert_not_nil(row[:float], "Missing float method")
      assert(Float === row[:float], "Incorrect float field value #{row[:float]} type")
      assert_not_nil(row[:date], "Missing date method")
      assert_equal(Date, row[:date].class, "Incorrect date field value #{row[:date]} type")
    end
  end

  def test_empty
    loader = Jinx::CsvIO.new(File.join(FIXTURES_DIR, 'empty.csv'))
    row = loader.shift
    assert_nil(row[:one], "Missing value not nil")
    assert_nil(row[:two], "Missing value not nil")
  end

  def test_accessor
    loader = Jinx::CsvIO.new(File.join(FIXTURES_DIR, 'variety.csv'))
    assert_equal(:id, loader.accessor('Id'), "Accessor incorrect")
    assert_equal(:string_field, loader.accessor('String Field'), "Accessor incorrect")
  end

  def test_write
    input = File.join(FIXTURES_DIR, 'variety.csv')
    output = File.join(OUTPUT_DIR, 'variety.csv')
    headers = records = nil
    # Read the input file content.
    File.open(input) do |file|
      headers = file.readline.chomp.split(/,\s*/)
      records = file.map { |line| line.chomp.split(/,\s*/) }
    end
    # Write the output file.
    Jinx::CsvIO.open(output, :mode => 'w', :headers => headers) do |csvio|
      records.each { |rec| csvio << rec }
    end
    # Compare the output to the input.
    File.open(output) do |file|
      assert_equal(headers, file.readline.chomp.split(/,\s*/), "Headers don't match")
      file.each_with_index do |line, i|
        rec = line.chomp.split(/,\s*/)
        assert_equal(records[i], rec, "Line #{i.succ} doesn't match")
      end
    end
  end
end