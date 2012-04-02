require 'set'

module Jinx
  module Csv
    # Merges two CSV files on common fields.
    class Joiner
      include Enumerable
  
      # @param [String, IO] source the join source
      # @param [String, IO] target the join target (default stdin)
      # @param [String, IO, nil] output the output file name or device (default stdout)
      def initialize(source, target=nil, output=nil)
        @source = source
        @target = target || STDIN
        @output = output || STDOUT
      end
  
      # Joins the source to the target and writes the output. The source fields used are
      # given by the +fields+ argument, if given. By default, all source fields are used.
      #
      # The output fields consist of the qualified source fields and all target fields.
      # The output fields are in the following order:
      # 1. The common fields, in order of occurrence in the source file.
      # 2. The qualified source-specific fields, in order of occurrence in the source file. 
      # 3. The target-specific fields, in order of occurrence in the target file. 
      #
      # The match is on the common qualified source and target fields.
      # Both files must be sorted in order of the common fields, sequenced by their
      # occurence in the source header.
      #
      # If an output argument is given, then the joined record is written to the output.
      # If a block is given, then the block is called on each record prior to writing
      # the record to the output.
      #
      # @param [<String>] fields the optional source fields to merge
      #   (default is all source fields)
      # @yield [rec] process the output
      # @yieldparam [FasterCSV::Record] rec the output record
      def join(*fields, &block)
        CsvIO.open(@target) do |tgt|
          CsvIO.open(@source) do |src|
            # all source fields (unordered)
            usflds = src.field_names.to_set
            fields.each do |fld|
              unless usflds.include?(fld) then
                raise ArgumentError.new("CSV join field #{fld} not found in the source file #{@sourc}.")
              end
            end
            # the qualified source fields (ordered)
            qsflds = fields.empty? ? src.field_names : fields
            tflds = tgt.field_names
            @common = qsflds & tflds
            # The headers consist of the common fields followed by the qualified
            # source-specific fields followed by the target-specific fields.
            hdrs = @common | qsflds | tflds
            CsvIO.open(@output, :mode => 'w', :headers => hdrs) do |out|
              merge(src, tgt, out, &block)
            end
          end
        end
    
        alias :each :join
      end
  
      private
    
      Buffer ||= Struct.new(:key, :record, :lookahead)
      
      # Merges the given source into the target as the output.
      # The output headers must be in the order specified by {#join}.
      #
      # @param [CsvIO] source the source CSV IO
      # @param [CsvIO] target the target CSV IO
      # @param [CsvIO] output the merged output CSV IO
      # @yield (see #join)
      # @yieldparam (see #join)
      # @see #join
      def merge(source, target, output)
        # the qualified source field accessors
        sflds = source.accessors & output.accessors
        # the target field accessors
        tflds = target.accessors
        # the common fields
        @common = sflds & tflds
        # The target-specific accessors
        trest = tflds - @common
        # The source-specific accessors
        srest = output.accessors - trest - @common
        # The output record
        orec = Array.new(output.accessors.size)
        # The source/target current/next (key, record) buffers
        # Read the first and second records into the buffers
        sbuf = shift(source)
        tbuf = shift(target)
        # Compare the source and target.
        while cmp = compare(sbuf, tbuf) do
          # Fill the output record in three sections: the common, source and target fields.
          orec.fill do |i|
            if i < @common.size then
              cmp <= 0 ? sbuf.key[i] : tbuf.key[i]
            elsif i < sflds.size then
              # Only fill the output record with source values if there is a current source
              # record and the target does not precede the source.
              sbuf.record[srest[i - @common.size]] if sbuf and cmp <= 0
            elsif tbuf and cmp >= 0
              # Only fill the output record with target values if there is a current target
              # record and the source does not precede the target.
              tbuf.record[trest[i - sflds.size]]
            end
          end
          yield orec if block_given?
          # Emit the output record.
          output << orec
          # Shift the buffers as necessary.
          ss, ts = shift?(sbuf, tbuf, cmp), shift?(tbuf, sbuf, -cmp)
          sbuf = shift(source, sbuf) if ss
          tbuf = shift(target, tbuf) if ts
        end
      end
    
      # Returns whether to shift the given buffer as follows:
      # * If the buffer precedes the other buffer, then true.
      # * If the buffer succeeds the other buffer, then false.
      # * Otherwise, if the lookahead record has the same key as the buffer record then true.
      # * Otherwise, if the other lookahead record has a different key than the other record, then true.
      #
      # @param [Buffer] buf the record buffer to check
      # @param [Buffer] other the other record buffer 
      # @param [-1, 0, 1] order the buffer comparison 
      # @return [Boolean] whether to shift the buffer
      def shift?(buf, other, order)
        case order
        when -1 then
          true
        when 1 then
          false
        when 0 then
          compare(buf, buf.lookahead) == 0 or compare(other, other.lookahead) != 0
        end
      end
    
      # Reads a record into the given buffers.
      #
      # @param [CsvIO] the open CSV stream to read
      # @param [Buffer, nil] cbuf the current record buffer
      # @return [Buffer, nil] the next current buffer, or nil if end of file 
      def shift(csvio, buf=nil)
        if buf then
          return if buf.lookahead.nil?
        else
          # prime the look-ahead
          buf = Buffer.new(nil, nil, look_ahead(csvio))
          return shift(csvio, buf)
        end
        buf.record = buf.lookahead.record
        buf.key = buf.lookahead.key
        buf.lookahead = look_ahead(csvio, buf.lookahead)
        buf
      end
    
      # @param [CsvIO] csvio the CSV file stream
      # @param [Buffer, nil] the look-ahead buffer
      # @return [Buffer, nil] the modified look-ahead, or nil if end of file
      def look_ahead(csvio, buf=nil)
        rec = csvio.next || return
        buf ||= Buffer.new
        buf.record = rec
        buf.key = @common.map { |k| rec[k] }
        buf
      end
    
      # Compares the given source and target buffers with result as follows:
      # * If source and target are nil, then nil
      # * If source is nil and target is not nil, then -1
      # * If target is nil and source is not nil, then 1
      # * Otherwise, the pair-wise comparison of the source and target keys
      #
      # @param [:key] the key holder
      # @return [-1, 0 , 1, nil] the comparison result 
      def compare(source, target)
        return target.nil? ? nil : 1 if source.nil?
        return -1 if target.nil?
        source.key.each_with_index do |v1, i|
          v2 = target.key[i]
          next if v1.nil? and v2.nil?
          return -1 if v1.nil?
          return 1 if v2.nil?
          cmp = v1 <=> v2
          return cmp unless cmp == 0
        end
        0
      end
    end
  end
end