require 'fileutils'
require 'faster_csv'
require 'jinx/helpers/options'
require 'jinx/helpers/collections'
require 'jinx/csv/joiner'

module Jinx
  # CsvIO reads or writes CSV records.
  # This class wraps a FasterCSV with the following modifications:
  # * relax the date parser to allow dd/mm/yyyy dates
  # * don't convert integer text with a leading zero to an octal number
  # * allow one custom converter with different semantics: if the converter block
  #   call returns nil, then continue conversion, otherwise return the converter
  #   result. This differs from FasterCSV converter semantics which calls converters
  #   as long the result equals the input field value. The CsvIO converter semantics
  #   supports converters that intend a String result to be the converted result.
  #
  # CsvIO is Enumerable, but does not implement the complete Ruby IO interface.
  class CsvIO
    include Enumerable

    # @return [<String>] the CSV field names
    attr_reader :field_names
  
    # @return [<Symbol>] the CSV field value accessor
    attr_reader :accessors
    alias :headers :accessors
  
    # Opens the CSV file and calls the given block with this CsvIO as the argument.
    #
    # @param (see #initialize)
    # @option (see #initialize)
    # @yield [csvio] the optional block to execute
    # @yieldparam [CsvIO] csvio the open CSVIO instance
    def self.open(dev, opts=nil)
      csvio = new(dev, opts)
      if block_given? then
        begin
          yield csvio
        ensure
          csvio.close
        end
      end
    end
  
    # Opens the given CSV file and calls {#each} with the given block.
    #
    # @param (see #initialize)
    # @option (see #initialize)
    # @yield [row] the block to execute on the row
    # @yieldparam [{Symbol => Object}] row the field symbol => value hash
    def self.foreach(file, opts=nil, &block)
      open(file, opts) { |csvio| csvio.each(&block) }
    end

    # Joins the source to the target and writes the output. The match is on all fields
    # held in common. If there is more than one match, then all but the first match has
    # empty values for the merged fields. Both files must be sorted in order of the
    # common fields, sequenced by their occurence in the source header.
    #
    # @param [String, IO] source the join source file
    # @param [{Symbol => String, IO, <String>}] opts the join options
    # @option opts [String, IO] :to the join target file name or device (default stdin)
    # @option opts [<String>] :for the target field names (default all target fields)
    # @option opts [String, IO] :as the output file name or device (default stdout)
    # @yield (see Csv::Joiner#join)
    # @yieldparam (see Csv::Joiner#join)
    def self.join(source, opts, &block)
      flds = opts[:for] || Array::EMPTY_ARRAY
      Csv::Joiner.new(source, opts[:to], opts[:as]).join(*flds, &block)
    end
    
    # Creates a new CsvIO for the specified source file.
    # If a converter block is given, then it is added to the CSV converters list.
    #
    # @param [String, IO] dev the CSV file or stream to open
    # @param [Hash] opts the open options
    # @option opts [String] :mode the input mode (default +r+)
    # @option opts [String] :headers the input field headers
    # @yield [value, info] converts the input value
    # @yieldparam [String] value the input value
    # @yieldparam info the current field's FasterCSV FieldInfo metadata
    # @raise [ArgumentError] if the input is nil
    def initialize(dev, opts=nil, &converter)
      raise ArgumentError.new("CSV input argument is missing") if dev.nil?
      # the CSV file open mode
      mode = Options.get(:mode, opts, 'r')
      # the CSV headers option; can be boolean or array
      hdr_opt = Options.get(:headers, opts)
      # there is a header record by default for an input CSV file
      hdr_opt ||= true if mode =~ /^r/
      # make parent directories if necessary for an output CSV file
      File.makedirs(File.dirname(dev)) if String == dev and mode =~ /^w/
      # if headers aren't given, then convert the input CSV header record names to underscore symbols
      hdr_cvtr = :symbol unless Enumerable === hdr_opt
      # make a custom converter
      custom = Proc.new { |value, info| convert(value, info, &converter) }
      # collect the options
      csv_opts = {:headers => hdr_opt, :header_converters => hdr_cvtr, :return_headers => true, :write_headers => true, :converters => custom}
      # Make the parent directory if necessary.
      FileUtils.mkdir_p(File.dirname(dev)) if String === dev and mode !~ /^r/
      # open the CSV file
      @csv = String === dev ? FasterCSV.open(dev, mode, csv_opts) : FasterCSV.new(dev, csv_opts)
      # the header => field name hash:
      # if the header option is set to true, then read the input header line.
      # otherwise, parse an empty string which mimics an input header line.
      hdr_row = case hdr_opt
      when true then
        @csv.shift
      when Enumerable then
        ''.parse_csv(:headers => hdr_opt, :header_converters => :symbol, :return_headers => true)
      else
        raise ArgumentError.new("CSV headers option value not supported: #{hdr_opt}")
      end
      # The field value accessors consist of the header row headers converted to a symbol.
      @accessors = hdr_row.headers
      # The field names consist of the header row values.
      @field_names = @accessors.map { |sym| hdr_row[sym] }
      # the header name => symbol map
      @hdr_sym_hash = hdr_row.to_hash.invert
    end
  
    # Closes the CSV file.
    def close
      @csv.close
    end

    # @param [String] header the CSV field header name
    # @param [Symbol] the header accessor method
    def accessor(name)
      @hdr_sym_hash[name]
    end
  
    # Iterates over each CSV row, yielding a row for each iteration.
    #
    # @yield [row] processes the CSV row
    # @yieldparam [FasterCSV::Row] row the CSV row
    def each(&block)
      @csv.each(&block)
    end
  
    # Reads the next CSV row.
    #
    # @return the next CSV row
    # @see #each
    def readline
      @csv.shift
    end

    alias :shift :readline

    alias :next :readline
  
    # Writes the given row to the CSV file.
    #
    #@param [{Symbol => Object}] row the input row
    def write(row)
      @csv << row
      @csv.flush
    end
  
    alias :<< :write
  
    private
  
    # 3-letter months => month sequence hash.
    MMM_MM_MAP = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'].to_compact_hash_with_index do |mmm, index|
      index < 9 ? ('0' + index.succ.to_s) : index.succ.to_s
    end
  
    # DateMatcher relaxes the FasterCSV DateMatcher to allow dd/mm/yyyy dates.
    DateMatcher = / \A(?: (\w+,?\s+)?\w+\s+\d{1,2},?\s+\d{2,4} | \d{1,2}-\w{3}-\d{2,4} | \d{4}[-\/]\d{1,2}[-\/]\d{1,2} | \d{1,2}[-\/]\d{1,2}[-\/]\d{2,4} )\z /x
  
    DD_MMM_YYYY_RE = /^(\d{1,2})-([[:alpha:]]{3})-(\d{2,4})$/
    
    # @param f the input field value to convert
    # @param info the CSV field info
    # @return the converted value
    def convert(f, info)
      return if f.nil?
      # the block has precedence
      value = yield(f, info) if block_given?
      # integer conversion
      value ||= Integer(f) if f =~ /^[1-9]\d*$/
      # date conversion
      value ||= convert_date(f) if f =~ CsvIO::DateMatcher
      # float conversion
      value ||= (Float(f) rescue f) if f =~ /^\d+\.\d*$/ or f =~ /^\d*\.\d+$/
      # return converted value or the input field if there was no conversion
      value || f
    end
  
    # @param [String] the input field value
    # @return [Date] the converted date
    def convert_date(f)
      # If input value is in dd-mmm-yy format, then reformat.
      # Otherwise, parse as a Date if possible.
      if f =~ DD_MMM_YYYY_RE then
        ddmmyy = reformat_dd_mmm_yy_date(f) || return
        convert_date(ddmmyy)
      else
        Date.parse(f, true) rescue nil
      end
    end
  
    # @param [String] the input field value in dd-mmm-yy format
    # @return [String] the reformatted date String in mm/dd/yy format
    def reformat_dd_mmm_yy_date(f)
      dd, mmm, yy = DD_MMM_YYYY_RE.match(f).captures
      mm = MMM_MM_MAP[mmm.downcase] || return
      "#{mm}/#{dd}/#{yy}"
    end
  end
end
