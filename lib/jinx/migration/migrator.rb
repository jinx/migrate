require 'yaml'
require 'jinx/csv/csvio'
require 'jinx/helpers/boolean'
require 'jinx/helpers/class'
require 'jinx/helpers/collections'
require 'jinx/helpers/lazy_hash'
require 'jinx/helpers/log'
require 'jinx/helpers/inflector'
require 'jinx/helpers/pretty_print'
require 'jinx/helpers/transitive_closure'
require 'jinx/migration/migratable'
require 'jinx/migration/reader'
require 'jinx/migration/filter'

module Jinx
  class MigrationError < RuntimeError; end

  # Migrates a CSV extract to a caBIG application.
  class Migrator
    include Enumerable

    # Creates a new Migrator from the given options.
    #
    # @param [{Symbol => Object}] opts the migration options
    # @option opts [Class] :target the required target domain class
    # @option opts [<String>, String] :mapping the required input field => caTissue attribute mapping file(s)
    # @option opts [String, Migration::Reader] :input the required input file name or an adapter which
    #   implements the {Migration::Reader} methods
    # @option opts [<String>, String] :defaults the optional caTissue attribute => value default mapping file(s)
    # @option opts [<String>, String] :filters the optional caTissue attribute input value => caTissue value filter file(s)
    # @option opts [<String>, String] :shims the optional shim file(s) to load
    # @option opts [String] :unique the optional flag which ensures that migrator calls the +uniquify+ method on
    #   those migrated objects whose class includes the +Unique+ module
    # @option opts [String] :create the optional flag indicating that existing target objects are ignored
    # @option opts [String] :bad the optional invalid record file
    # @option opts [String, IO] :extract the optional extract file or object that responds to +<<+
    # @option opts [<String>] :extract_headers the optional extract CSV field headers
    # @option opts [Integer] :from the optional starting source record number to process
    # @option opts [Integer] :to the optional ending source record number to process
    # @option opts [Boolean] :quiet the optional flag which suppress output messages
    # @option opts [Boolean] :verbose the optional flag to print the migration progress
    def initialize(opts)
      @rec_cnt = 0
      @mgt_mths = {}
      parse_options(opts)
      build
    end

    # Imports this migrator's CSV file and calls the given block on each migrated target
    # domain object. If no block is given, then this method returns an array of the
    # migrated target objects.
    #
    # @yield [target, row] operates on the migration target
    # @yieldparam [Resource] target the migrated target domain object
    # @yieldparam [{Symbol => Object}] row the migration source record
    def migrate(&block)
      unless block_given? then
        return migrate { |tgt, row| tgt }
      end
      # If there is an extract, then wrap the migration in an extract
      # writer block.
      if @extract then
        if String === @extract then
          logger.debug { "Opening migration extract #{@extract}..." }
          FileUtils::mkdir_p(File.dirname(@extract))
          if @extract_hdrs then
            logger.debug { "Migration extract headers: #{@extract_hdrs.join(', ')}." }
            CsvIO.open(@extract, :mode => 'w', :headers => @extract_hdrs) do |io|
              @extract = io
              return migrate(&block)
            end
          else
            File.open(@extract, 'w') do |io|
              @extract = io
              return migrate(&block)
            end
          end
        end
        # Copy the extract into a local variable and clear the extract i.v.
        # prior to a recursive call with an extract writer block.
        io, @extract = @extract, nil
        return migrate do |tgt, row|
          res = yield(tgt, row)
          tgt.extract(io)
          res
        end
      end
      begin
        migrate_rows(&block)
      ensure
        @rejects.close if @rejects
        remove_migration_methods
      end
    end

    # @yield [target] iterate on each migration target
    # @yieldparam [Jinx::Resource] the migration target
    def each
      migrate { |tgt, row| yield tgt }
    end

    private
    
    # Cleans up after the migration by removing the methods injected by migration
    # shims.
    def remove_migration_methods
      # remove the migrate_<attribute> methods
      @mgt_mths.each do | klass, hash|
        hash.each_value do |sym|
          while klass.method_defined?(sym)
            klass.instance_method(sym).owner.module_eval { remove_method(sym) }
          end
        end
      end
      # remove the migrate method
      @creatable_classes.each do |klass|
        while (k = klass.instance_method(:migrate).owner) < Migratable
          k.module_eval { remove_method(:migrate) }
        end
      end
      # remove the target extract method
      remove_extract_method(@target) if @extract
    end
    
    def remove_extract_method(klass)
      if (klass.method_defined?(:extract)) then
        klass.module_eval { remove_method(:extract) }
        sc = klass.superclass
        remove_extract_method(sc) if sc < Migratable
      end
    end

    def parse_options(opts)
      @fld_map_files = opts[:mapping]
      if @fld_map_files.nil? then
        Jinx.fail(MigrationError, "Migrator missing required field mapping file parameter")
      end
      @def_files = opts[:defaults]
      @flt_files = opts[:filters]
      shims_opt = opts[:shims] ||= []
      # Make a single shims file into an array.
      @shims = shims_opt.collection? ? shims_opt : [shims_opt]
      @unique = opts[:unique]
      @from = opts[:from] ||= 1
      @input = opts[:input]
      if @input.nil? then
        Jinx.fail(MigrationError, "Migrator missing required source file parameter")
      end
      @target_class = opts[:target]
      if @target_class.nil? then
        Jinx.fail(MigrationError, "Migrator missing required target class parameter")
      end
      @bad_file = opts[:bad]
      @extract = opts[:extract]
      @extract_hdrs = opts[:extract_headers]
      @create = opts[:create]
      logger.info("Migration options: #{printable_options(opts).pp_s}.")
      # flag indicating whether to print a progress monitor
      @verbose = opts[:verbose]
    end
    
    def printable_options(opts)
      popts = opts.reject { |option, value| value.nil_or_empty? }
      # The target class should be a simple class name rather than the class metadata.
      popts[:target] = popts[:target].qp if popts.has_key?(:target)
      popts
    end

    def build
      # the current source class => instance map
      Jinx.fail(MigrationError, "No file to migrate") if @input.nil?

      # If the input is a file name, then make a CSV loader which only converts input fields
      # corresponding to non-String attributes.
      if String === @input then
        @reader = CsvIO.new(@input, &method(:convert))
        logger.debug { "Migration data input file #{@input} headers: #{@reader.headers.qp}" }
      else
        @reader = @input
      end
      
      # add shim modifiers
      load_shims(@shims)

      # create the class => path => default value hash
      @def_hash = @def_files ? load_defaults_files(@def_files) : {}
      # create the class => path => default value hash
      @flt_hash = @flt_files ? load_filter_files(@flt_files) : {}
      # the missing owner classes
      @owners = Set.new
      # create the class => path => header hash
      fld_map = load_field_map_files(@fld_map_files)
      # create the class => paths hash
      @cls_paths_hash = create_class_paths_hash(fld_map, @def_hash)
      # create the path => class => header hash
      @header_map = create_header_map(fld_map)
      # Order the creatable classes by dependency, owners first, to smooth the migration process.
      @creatable_classes = @cls_paths_hash.keys.sort do |klass, other|
        other.depends_on?(klass) ? -1 : (klass.depends_on?(other) ? 1 : 0)
      end
      # An abstract class cannot be instantiated.
      @creatable_classes.each do |klass|
        if klass.abstract? then
          Jinx.fail(MigrationError, "Migrator cannot create the abstract class #{klass}; specify a subclass instead in the mapping file.")
        end
      end
      
      logger.info { "Migration creatable classes: #{@creatable_classes.qp}." }
      unless @def_hash.empty? then logger.info { "Migration defaults: #{@def_hash.qp}." } end
      
      # the class => attribute migration methods hash
      create_migration_method_hashes
      
      # Print the input field => attribute map and collect the String input fields for
      # the custom CSVLoader converter.
      @nonstring_headers = Set.new
      logger.info("Migration attributes:")
      @header_map.each do |path, cls_hdr_hash|
        prop = path.last
        cls_hdr_hash.each do |klass, hdr|
          type_s = prop.type ? prop.type.qp : 'Object'
          logger.info("  #{hdr} => #{klass.qp}.#{path.join('.')} (#{type_s})")
        end
        @nonstring_headers.merge!(cls_hdr_hash.values) if prop.type != Java::JavaLang::String
      end
    end
   
    # Converts the given input field value as follows:
    # * If the info header is a String field, then return the value unchanged.
    # * Otherwise, return nil which will delegate to the generic CsvIO converter.
    # @param (see CsvIO#convert)
    # @yield (see CsvIO#convert)
    def convert(value, info)
      value unless @nonstring_headers.include?(info.header)
    end
    
    # Adds missing owner classes to the migration class path hash (with empty paths)
    # for the classes in the given hash.
    #
    # @param [{Class => Object}] hash the class map
    # @yield the map entry for a new owner
    def add_owners(hash, &factory)
      hash.keys.each { |klass| add_owners_for(klass, hash, &factory) }
    end
    
    # Adds missing owner classes to the migration class path hash (with empty paths)
    # for the given migration class.
    #
    # @param [Class] klass the migration class
    # @param [{Class => Object}] hash the class map
    # @yield the map entry for a new owner
    def add_owners_for(klass, hash, &factory)
      owner = missing_owner_for(klass, hash) || return
      logger.debug { "The migrator is adding #{klass.qp} owner #{owner}..." }
      @owners << owner
      hash[owner] = yield
      add_owners_for(owner, hash, &factory)
    end
    
    # @param [Class] klass the migration class
    # @param [{Class => Object}] hash the class map
    # @return [Class, nil] the missing class owner, if any
    def missing_owner_for(klass, hash)
      # check for an owner among the current migration classes
      return if klass.owners.any? do |owner|
        hash.detect_key { |other| other <= owner }
      end
      # Find the first non-abstract candidate owner that is a dependent
      # of a migrated class.
      klass.owners.detect do |owner|
        not owner.abstract? and hash.detect_key { |other| owner.depends_on?(other, true) }
      end
    end

    # Creates the class => +migrate_+_<attribute>_ hash for the given klasses.
    def create_migration_method_hashes
      # the class => attribute => migration filter hash
      @attr_flt_hash = {}
      customizable_class_attributes.each do |klass, pas|
        flts = migration_filters(klass) || next
        @attr_flt_hash[klass] = flts
      end
      # print the migration shim methods
      unless @mgt_mths.empty? then
        logger.info("Migration shim methods:\n#{@mgt_mths.qp}")
      end
    end

    # @return the class => attributes hash for terminal path attributes which can be customized by +migrate_+ methods
    def customizable_class_attributes
      # The customizable classes set, starting with creatable classes and adding in
      # the migration path terminal attribute declarer classes below.
      klasses = @creatable_classes.to_set
      # the class => path terminal attributes hash
      cls_attrs_hash = LazyHash.new { Set.new }
      # add each path terminal attribute and its declarer class
      @cls_paths_hash.each_value do |paths|
        paths.each do |path|
          prop = path.last
          type = prop.declarer
          klasses << type
          cls_attrs_hash[type] << prop
        end
      end
      
      # Merge each redundant customizable superclass into its concrete customizable subclasses. 
      klasses.dup.each do |cls|
        redundant = false
        klasses.each do |other|
          # cls is redundant if it is a superclass of other
          redundant = other < cls
          if redundant then
            cls_attrs_hash[other].merge!(cls_attrs_hash[cls])
          end
        end
        # remove the redundant class
        if redundant then
          cls_attrs_hash.delete(cls)
          klasses.delete(cls)
        end
      end
      
      cls_attrs_hash
    end

    # Discovers methods of the form +migrate+__attribute_ implemented for the paths
    # in the given class => paths hash the given klass. The migrate method is called
    # on the input field value corresponding to the path.
    def migration_filters(klass)
      # the attribute => migration method hash
      mh = attribute_method_hash(klass)
      @mgt_mths[klass] = mh unless mh.empty?
      fh = attribute_filter_hash(klass)
      return if mh.empty? and fh.empty?
      # For each class path terminal attribute metadata, add the migration filters
      # to the attribute metadata => proc hash.
      klass.attributes.to_compact_hash do |pa|
        # the filter
        flt = fh[pa]
        # the migration shim method
        mth = mh[pa]
        # the filter proc
        Proc.new do |obj, value, row|
          # filter the value
          value = flt.transform(value) if flt and not value.nil?
          # apply the migrate_<attribute> method, if defined
          if mth then
            obj.send(mth, value, row) unless value.nil?
          else
            value
          end
        end
      end
    end
    
    def attribute_method_hash(klass)
      # the migrate methods, excluding the Migratable migrate_references method
      mths = klass.instance_methods(true).select { |mth| mth =~ /^migrate.(?!references)/ }
      # the attribute => migration method hash
      mh = {}
      mths.each do |mth|
        # the attribute suffix, e.g. name for migrate_name or Name for migrateName
        suffix = /^migrate(_)?(.*)/.match(mth).captures[1]
        # the attribute name
        attr_nm = suffix[0, 1].downcase + suffix[1..-1]
        # the attribute for the name, or skip if no such attribute
        pa = klass.standard_attribute(attr_nm) rescue next
        # associate the attribute => method
        mh[pa] = mth
      end
      mh
    end
    
    # Builds the property => filter hash. The filter is specified in the +--filter+ migration
    # option. A Boolean property has a default String => Boolean filter which converts the
    # input string to a Boolean as specified in the +Jinx::Boolean+ +to_boolean+ methods.
    #
    # @param [Class] klass the migration class
    # @return [Property => Proc] the filter migration methods
    def attribute_filter_hash(klass)
      hash = @flt_hash[klass]
      fh = {}
      klass.each_property do |prop|
        pa = prop.attribute
        spec = hash[pa] if hash
        # If the property is boolean, then make a filter that operates on the parsed string input.
        if prop.type == Java::JavaLang::Boolean then
          fh[pa] = boolean_filter(spec)
          logger.debug { "The migrator added the default text -> boolean filter for #{klass.qp} #{pa}." }
        elsif spec then
          fh[pa] = Migration::Filter.new(spec)
        end
      end
      unless fh.empty? then
        logger.debug { "The migration filters were loaded for #{klass.qp} #{fh.keys.to_series}." }
      end
      fh
    end
                    
    # @param [String, nil] the value filter, if any
    # @return [Migration::Filter] the boolean property migration filter
    def boolean_filter(spec=nil)
      # break up the spec into two specs, one on strings and one on booleans
      bspec, sspec = spec.split { |k, v| Boolean === k } if spec
      bf = Migration::Filter.new(bspec) if bspec and not bspec.empty?
      sf = Migration::Filter.new(sspec) if sspec and not sspec.empty?
      # make the composite filter 
      Migration::Filter.new do |value|
        fv = sf.transform(value) if sf
        if fv.nil? then
          bv = Jinx::Boolean.for(value) rescue nil
          fv = bf.nil? || bv.nil? ? bv : bf.transform(bv)
        end
        fv
      end 
    end

    # Loads the shim files.
    #
    # @param [<String>, String] files the file or file array
    def load_shims(files)
      logger.debug { "Loading the migration shims with load path #{$:.pp_s}..." }
      files.enumerate do |file|
        load file
        logger.info { "The migrator loaded the shim file #{file}." }
      end
    end

    # Migrates all rows in the input.
    #
    # @yield (see #migrate)
    # @yieldparam (see #migrate)
    def migrate_rows
      # open an CSV output for rejects if the bad option is set
      if @bad_file then
        @rejects = open_rejects(@bad_file)
        logger.info("Unmigrated records will be written to #{File.expand_path(@bad_file)}.")
      end
      
      @rec_cnt = mgt_cnt = 0
      logger.info { "Migrating #{@input}..." }
      puts "Migrating #{@input}..." if @verbose
      @reader.each do |row|
        # the one-based current record number
        rec_no = @rec_cnt + 1
        # skip if the row precedes the from option
        if rec_no == @from and @rec_cnt > 0 then
          logger.info("Skipped the initial #{@rec_cnt} records.")
        elsif rec_no == @to then
          logger.info("Ending the migration after processing record #{@rec_cnt}.")
          return
        elsif rec_no < @from then
          @rec_cnt += 1
          next
        end
        begin
          # migrate the row
          logger.debug { "Migrating record #{rec_no}..." }
          tgt = migrate_row(row)
          # call the block on the migrated target
          if tgt then
            logger.debug { "The migrator built #{tgt} with the following content:\n#{tgt.dump}" }
            yield(tgt, row)
          end
        rescue Exception => e
          logger.error("Migration error on record #{rec_no} - #{e.message}:\n#{e.backtrace.pp_s}")
          # If there is a reject file, then don't propagate the error.
          raise unless @rejects
          # try to clear the migration state
          clear(tgt) rescue nil
          # clear the target
          tgt = nil
        end
        if tgt then
          # replace the log message below with the commented alternative to detect a memory leak
          logger.info { "Migrated record #{rec_no}." }
          #memory_usage = `ps -o rss= -p #{Process.pid}`.to_f / 1024 # in megabytes
          #logger.debug { "Migrated rec #{@rec_cnt}; memory usage: #{sprintf("%.1f", memory_usage)} MB." }
          mgt_cnt += 1
          if @verbose then print_progress(mgt_cnt) end
          # clear the migration state
          clear(tgt)
        elsif @rejects then
          # If there is a rejects file then warn, write the reject and continue.
          logger.warn("Migration not performed on record #{rec_no}.")
          @rejects << row
          @rejects.flush
          logger.debug("Invalid record #{rec_no} was written to the rejects file #{@bad_file}.")
        else
          Jinx.fail(MigrationError, "Migration not performed on record #{rec_no}")
        end
        # Bump the record count.
        @rec_cnt += 1
      end
      logger.info("Migrated #{mgt_cnt} of #{@rec_cnt} records.")
      if @verbose then
        puts
        puts "Migrated #{mgt_cnt} of #{@rec_cnt} records."
      end
    end
    
    # Makes the rejects CSV output file.
    #
    # @param [String] file the output file
    # @return [IO] the reject stream
    def open_rejects(file)
      # Make the parent directory.
      FileUtils.mkdir_p(File.dirname(file))
      # Open the file.
      FasterCSV.open(file, 'w', :headers => true, :header_converters => :symbol, :write_headers => true)
    end
    
    # Prints a '+' progress indicator after each migrated record to stdout.
    #
    # @param [Integer] count the migrated record count
    def print_progress(count)
      # If the line is 72 characters, then print a line break 
      puts if count % 72 == 0
      # Print the progress indicator
      print "+"
    end

    # Clears references to objects allocated for migration of a single row into the given target.
    # This method does nothing. Subclasses can override.
    #
    # This method is overridden by subclasses to clear the migration state to conserve memory,
    # since this migrator should consume O(1) rather than O(n) memory for n migration records.
    def clear(target)
    end

    # Imports the given CSV row into a target object.
    #
    # @param [{Symbol => Object}] row the input row field => value hash
    # @return the migrated target object if the migration is valid, nil otherwise
    def migrate_row(row)
      # create an instance for each creatable class
      created = Set.new
      # the migrated objects
      migrated = @creatable_classes.map { |klass| create_instance(klass, row, created) }
      # migrate each object from the input row
      migrated.each do |obj|
        # First uniquify the object if necessary.
        if @unique and Unique === obj then
          logger.debug { "The migrator is making #{obj} unique..." }
          obj.uniquify
        end
        obj.migrate(row, migrated)
      end
      # the valid migrated objects
      @migrated = migrate_valid_references(row, migrated)
      # the candidate target objects
      tgts = @migrated.select { |obj| @target_class === obj }
      if tgts.size > 1 then
        raise MigrationError.new("Ambiguous #{@target_class} targets #{tgts.to_series}")
      end
      target = tgts.first || return
      
      logger.debug { "Migrated target #{target}." }
      target
    end
    
    # Sets the migration references for each valid migrated object.
    #
    # @param row (see #migrate_row)
    # @param [Array] migrated the migrated objects
    # @return [Array] the valid migrated objects
    def migrate_valid_references(row, migrated)
      # Split the valid and invalid objects. The iteration is in reverse dependency order,
      # since invalidating a dependent can invalidate the owner.
      ordered = migrated.transitive_closure(:dependents)
      ordered.keep_if { |obj| migrated.include?(obj) }.reverse!
      valid, invalid = ordered.partition do |obj|
        if migration_valid?(obj) then
          obj.migrate_references(row, migrated, @target_class, @attr_flt_hash[obj.class])
          true
        else
          obj.class.owner_attributes.each { |pa| obj.clear_attribute(pa) }
          false
        end
      end
      
      # Go back through the valid objects in dependency order to invalidate dependents
      # whose owner is invalid.
      valid.reverse.each do |obj|
        unless owner_valid?(obj, valid, invalid) then
          invalid << valid.delete(obj)
          logger.debug { "The migrator invalidated #{obj} since it does not have a valid owner." }
        end
      end
      
      # Go back through the valid objects in reverse dependency order to invalidate owners
      # created only to hold a dependent which was subsequently invalidated.
      valid.reject do |obj|
        if @owners.include?(obj.class) and obj.dependents.all? { |dep| invalid.include?(dep) } then
          # clear all references from the invalidated owner
          obj.class.domain_attributes.each { |pa| obj.clear_attribute(pa) }
          invalid << obj
          logger.debug { "The migrator invalidated #{obj.qp} since it was created solely to hold subsequently invalidated dependents." }
          true
        end
      end
    end
    
    # Returns whether the given domain object satisfies at least one of the following conditions:
    # * it does not have an owner among the invalid objects
    # * it has an owner among the valid objects
    #
    # @param [Resource] obj the domain object to check
    # @param [<Resource>] valid the valid migrated objects
    # @param [<Resource>] invalid the invalid migrated objects
    # @return [Boolean] whether the owner is valid 
    def owner_valid?(obj, valid, invalid)
      otypes = obj.class.owners
      invalid.all? { |other| not otypes.include?(other.class) } or
        valid.any? { |other| otypes.include?(other.class) }
    end

    # @param [Migratable] obj the migrated object
    # @return [Boolean] whether the migration is successful
    def migration_valid?(obj)
      if obj.migration_valid? then
        true
      else
        logger.debug { "The migrated #{obj.qp} is invalid." }
        false
      end
    end

    # Creates an instance of the given klass from the given row.
    # The new klass instance and all intermediate migrated instances are added to the
    # created set.
    #
    # @param [Class] klass
    # @param [{Symbol => Object}] row the input row
    # @param [<Resource>] created the migrated instances for this row
    # @return [Resource] the new instance
    def create_instance(klass, row, created)
      # the new object
      logger.debug { "The migrator is building #{klass.qp}..." }
      created << obj = klass.new
      migrate_properties(obj, row, created)
      add_defaults(obj, row, created)
      logger.debug { "The migrator built #{obj}." }
      obj
    end
    
    # Migrates each input field to the associated domain object attribute.
    # String input values are stripped. Missing input values are ignored.
    #
    # @param [Resource] the migration object
    # @param row (see #create)
    # @param [<Resource>] created (see #create)
    def migrate_properties(obj, row, created)
      # for each input header which maps to a migratable target attribute metadata path,
      # set the target attribute, creating intermediate objects as needed.
      @cls_paths_hash[obj.class].each do |path|
        header = @header_map[path][obj.class]
        # the input value
        value = row[header]
        value.strip! if String === value
        next if value.nil?
        # fill the reference path
        ref = fill_path(obj, path[0...-1], row, created)
        # set the attribute
        migrate_property(ref, path.last, value, row)
      end
    end
    
    # @param [Resource] the migration object
    # @param row (see #create)
    # @param [<Resource>] created (see #create)
    def add_defaults(obj, row, created)
      dh = @def_hash[obj.class] || return
      dh.each do |path, value|
        # fill the reference path
        ref = fill_path(obj, path[0...-1], row, created)
        # set the attribute to the default value unless there is already a value
        ref.merge_attribute(path.last.to_sym, value)
      end
    end

    # Fills the given reference Property path starting at obj.
    #
    # @param row (see #create)
    # @param created (see #create)
    # @return the last domain object in the path
    def fill_path(obj, path, row, created)
      # create the intermediate objects as needed (or return obj if path is empty)
      path.inject(obj) do |parent, prop|
        # the referenced object
        parent.send(prop.reader) or create_reference(parent, prop, row, created)
      end
    end

    # Sets the given migrated object's reference attribute to a new referenced domain object.
    #
    # @param [Resource] obj the domain object being migrated
    # @param [Property] property the property being migrated
    # @param row (see #create)
    # @param created (see #create)
    # @return the new object
    def create_reference(obj, property, row, created)
      if property.type.abstract? then
        Jinx.fail(MigrationError, "Cannot create #{obj.qp} #{property} with abstract type #{property.type}")
      end
      ref = property.type.new
      ref.migrate(row, Array::EMPTY_ARRAY)
      obj.send(property.writer, ref)
      created << ref
      logger.debug { "The migrator created #{obj.qp} #{property} #{ref}." }
      ref
    end

    # Sets the given property value to the filtered input value. If there is a filter
    # defined for the property, then that filter is applied. If there is a migration
    # shim method with name +migrate_+_attribute_, then that method is called on the
    # (possibly filtered) value. The target object property is set to the resulting
    # filtered value. 
    #
    # @param [Migratable] obj the target domain object
    # @param [Property] property the property to set
    # @param value the input value
    # @param [{Symbol => Object}] row the input row
    def migrate_property(obj, property, value, row)
      # if there is a shim migrate_<attribute> method, then call it on the input value
      value = filter_value(obj, property, value, row)
      return if value.nil?
      # set the attribute
      begin
        obj.send(property.writer, value)
      rescue Exception => e
        Jinx.fail(MigrationError, "Could not set #{obj.qp} #{property} to #{value.qp}", e)
      end
      logger.debug { "Migrated #{obj.qp} #{property} to #{value}." }
    end
    
    # Calls the shim migrate_<attribute> method or config filter on the input value.
    #
    # @param value the input value
    # @param [Property] property the property to set
    # @return the input value, if there is no filter, otherwise the filtered value
    def filter_value(obj, property, value, row)
      flt = filter_for(obj, property.to_sym)
      return value if flt.nil?
      fval = flt.call(obj, value, row)
      unless value == fval then
        logger.debug { "The migration filter transformed the #{obj.qp} #{property} value from #{value.qp} to #{fval}." }
      end
      fval
    end
    
    def filter_for(obj, attribute)
      flts = @attr_flt_hash[obj.class] || return
      flts[attribute]
    end
    
    def current_record
      @rec_cnt + 1
    end

    # @param [<String>, String] files the migration fields mapping file or file array
    # @return [{Class => {Property => Symbol}}] the class => path => header hash
    #   loaded from the mapping files
    def load_field_map_files(files)
      map = LazyHash.new { Hash.new }
      files.enumerate { |file| load_field_map_file(file, map) }

      # include the target class
      map[@target_class] ||= Hash.new
      # add the default classes
      @def_hash.each_key { |klass| map[klass] ||= Hash.new }
      # add the owners
      add_owners(map) { Hash.new }

      # Include only concrete classes that are not a superclass of another migration class.
      classes = map.keys
      sub_hash = classes.to_compact_hash do |klass|
        subs = classes.select { |other| other < klass }
        subs.delete_if { |klass| subs.any? { |other| other < klass } }
      end
      
      # Merge the superclass paths into the subclass paths.
      sub_hash.each do |klass, subs|
        paths = map.delete(klass)
        # Add, but don't replace, path => header entries from the superclass.
        subs.each do |sub|
          map[sub].merge!(paths) { |key, old, new| old }
          logger.debug { "Migrator merged #{klass.qp} mappings into the subclass #{sub.qp}." }
        end
      end
      
      # Validate that there are no abstract classes in the mapping.
      map.each_key do |klass|
        if klass.abstract? then
          raise MigrationError.new("Cannot migrate to the abstract class #{klass}")
        end
      end

      map
    end
    
    # @param [String] file the migration fields configuration file
    # @param [{Class => {Property => Symbol}}] hash the class => path => header hash
    #   to populate from the loaded configuration
    def load_field_map_file(file, hash)
      # load the field mapping config file
      begin
        config = YAML.load_file(file)
      rescue
        Jinx.fail(MigrationError, "Could not read field map file #{file}: " + $!)
      end
      populate_field_map(config, hash)
    end
    
    # @param [{String => String}] config the attribute => header specification
    # @param hash (see #load_field_map_file)
    def populate_field_map(config, hash)
      # collect the class => path => header entries
      config.each do |field, attr_list|
        next if attr_list.blank?
        # the header accessor method for the field
        header = @reader.accessor(field)
        if header.nil? then
          Jinx.fail(MigrationError, "Field defined in migration configuration not found in input file #{@input} headers: #{field}")
        end
        # associate each attribute path in the property value with the header
        attr_list.split(/,\s*/).each do |path_s|
          klass, path = create_attribute_path(path_s)
          hash[klass][path] = header
        end
      end
    end
    
    # Loads the defaults configuration files.
    #
    # @param [<String>, String] files the file or file array to load
    # @return [<Class => <String => Object>>] the class => path => default value entries 
    def load_defaults_files(files)
      # collect the class => path => value entries from each defaults file
      hash = LazyHash.new { Hash.new }
      files.enumerate { |file| load_defaults_file(file, hash) }
      hash
    end
    
    # Loads the defaults config file into the given hash.
    #
    # @param [String] file the file to load
    # @param [<Class => <String => Object>>] hash the class => path => default value entries 
    def load_defaults_file(file, hash)
      begin
        config = YAML::load_file(file)
      rescue
        Jinx.fail(MigrationError, "Could not read defaults file #{file}: " + $!)
      end
      # collect the class => path => value entries
      config.each do |path_s, value|
        next if value.nil_or_empty?
        klass, path = create_attribute_path(path_s)
        hash[klass][path] = value
      end
    end    
    # Loads the filter config files.
    #
    # @param [<String>, String] files the file or file array to load
    # @return [<Class => <String => Object>>] the class => path => default value entries 
    def load_filter_files(files)
      # collect the class => path => value entries from each defaults file
      hash = {}
      files.enumerate { |file| load_filter_file(file, hash) }
      logger.debug { "The migrator loaded the filters #{hash.qp}." }
      hash
    end
    
    # Loads the filter config file into the given hash.
    #
    # @param [String] file the file to load
    # @param [<Class => <String => <Object => Object>>>] hash the class => path => input value => caTissue value entries 
    def load_filter_file(file, hash)
      # collect the class => attribute => filter entries
      logger.debug { "Loading the migration filter configuration #{file}..." }
      begin
        config = YAML::load_file(file)
      rescue
        Jinx.fail(MigrationError, "Could not read filter file #{file}: " + $!)
      end
      config.each do |path_s, flt|
        next if flt.nil_or_empty?
        klass, path = create_attribute_path(path_s)
        unless path.size == 1 then
          Jinx.fail(MigrationError, "Migration filter configuration path not supported: #{path_s}")
        end
        pa = klass.standard_attribute(path.first.to_sym)
        flt_hash = hash[klass] ||= {}
        flt_hash[pa] = flt
      end
    end

    # @param [String] path_s a period-delimited path string path_s in the form _class_(._attribute_)+
    # @return [<Property>] the corresponding attribute metadata path
    # @raise [MigrationError] if the path string is malformed or an attribute is not found
    def create_attribute_path(path_s)
      names = path_s.split('.')
      # If the path starts with a capitalized class name, then resolve the class.
      # Otherwise, the target class is the start of the path.
      klass = names.first =~ /^[A-Z]/ ? class_for_name(names.shift) : @target_class
      # There must be at least one attribute.
      if names.empty? then
        Jinx.fail(MigrationError, "Property entry in migration configuration is not in <class>.<attribute> format: #{path_s}")
      end
      
      # Build the attribute path.
      path = []
      names.inject(klass) do |parent, name|
        pa = name.to_sym
        prop = begin
          parent.property(pa)
        rescue NameError => e
          Jinx.fail(MigrationError, "Migration field mapping attribute #{parent}.#{pa} not found", e)
        end
        if prop.collection? then
          Jinx.fail(MigrationError, "Migration field mapping attribute #{parent}.#{prop} is a collection, which is not supported")
        end
        path << prop
        prop.type
      end
      
      # Return the starting class and Property path.
      # Note that the starting class is not necessarily the first path attribute declarer, since the
      # starting class could be the concrete target class rather than an abstract declarer. this is
      # important, since the class must be instantiated.
      [klass, path]
    end
    
    # The context module is given by the target class {ResourceClass#domain_module}.
    #
    # @return [Module] the class name resolution context
    def context_module
      @target_class.domain_module
    end
    
    # @param [String] the class name to resolve in the context of this migrator
    # @return [Class] the corresponding class
    # @raise [NameError] if the name cannot be resolved
    def class_for_name(name)
      context_module.module_for_name(name)
    end

    # @return a new class => [paths] hash from the migration fields configuration map
    def create_class_paths_hash(fld_map, def_map)
      hash = {}
      fld_map.each { |klass, path_hdr_hash| hash[klass] = path_hdr_hash.keys.to_set }
      def_map.each { |klass, path_val_hash| (hash[klass] ||= Set.new).merge(path_val_hash.keys) }
      hash
    end

    # @return a new path => class => header hash from the migration fields configuration map
    def create_header_map(fld_map)
      hash = LazyHash.new { Hash.new }
      fld_map.each do |klass, path_hdr_hash|
        path_hdr_hash.each { |path, hdr| hash[path][klass] = hdr }
      end
      hash
    end
  end
end