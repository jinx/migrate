require 'jinx/helpers/validation'

module Jinx
  module Migration
    # Transforms input values to a result based on a migration filter configuration.
    # Each configuration entry is one of the following:
    #   * literal: literal
    #   * regexp: literal
    #   * regexp: template
    #
    # The regexp template can include match references (+$1+, +$2+, etc.) corresponding to the regexp captures.
    # If the input value equals a literal, then the mapped literal is returned. Otherwise, if the input value
    # matches a regexp, then the mapped transformation is returned after reference substitution. Otherwise,
    # the input value is returned unchanged.
    #
    # For example, the config:
    #   /(\d{1,2})\/x\/(\d{1,2})/ : $1/15/$2
    #   n/a : ~
    # converts the input value as follows:
    #   3/12/02 => 3/12/02 (no match)
    #   5/x/04 => 5/15/04
    #   n/a => nil
    #
    # A catch-all +/.*/+ regexp transforms any value which does not match another value or regexp, e.g.:
    #   /^(\d+(\.\d*)?)( g(ram)?s?)?$/ : $1 
    #   /.*/ : 0
    # converts the input value as follows:
    #   3 => 3
    #   4.3 grams => 4.3
    #   unknown => 0
    class Filter
      # Builds the filter proc from the given specification or block.
      # If both a specification and a block are given, then the block is applied before
      # the specificiation.  
      #
      # @param [String] spec the filter configuration specification.
      # @yield [value] converts the input field value into a caTissue property value
      # @yieldparam value the CSV input value 
      def initialize(spec=nil, &block)
        @proc = spec ? to_proc(spec, &block) : block
        raise ArgumentError.new("Migration filter is missing both a specification and a block") if @proc.nil?
      end
        
      # @param [String] value the input string
      # @return the transformed result
      def transform(value)
        @proc.call(value)
      end
    
      private
    
      # The pattern to match a regular expression with captures.
      # @private
      REGEXP_PAT = /^\/(.*[^\\])\/([inx]+)?$/
      
      # Builds the filter proc from the given specification.  
      # If both a specification and a block are given, then the block is applied before
      # the specificiation.  
      #
      # @param (see #initialize)
      # @yield (see #initialize)
      # @yieldparam (see #initialize)
      # @return [Proc] a proc which convert the input field value into a caTissue property value
      def to_proc(spec=nil, &block)
        # Split the filter spec into a straight value => value hash and a pattern => value hash.
        ph, vh = spec.split { |k, v| k =~ REGEXP_PAT }
        # The Regexp => value hash is built from the pattern => value hash.
        reh = regexp_hash(ph)
        # The value proc.
        value_proc(reh, vh)
      end
      
      # @param {Regexp => (Object, <Integer>)} regexp_hash the regexp => (result, indexes) hash
      # @param {String => Object} value_hash the value => result hash
      # @yield (see #to_proc)
      # @yieldparam (see #to_proc)
      # @return [Proc] a proc which convert the input field value into a caTissue property value
      def value_proc(regexp_hash, value_hash)
        # The new proc matches preferentially on the literal value, then the first matching regexp.
        # If no match on either a literal or a regexp, then the value is preserved.
        Proc.new do |value|
          value = yield(value) if block_given?
          if value_hash.has_key?(value) then
            value_hash[value]
          else
            # The first regex which matches the value.
            regexp = regexp_hash.detect_key { |re| value =~ re }
            # If there is a match, then apply the filter to the match data.
            # Otherwise, pass the value through unmodified.
            if regexp then
              reval, ndxs = regexp_hash[regexp]
              if ndxs.empty? or not String === reval then
                reval
              else
                # The match captures (cpts[i - 1] is $i match).
                cpts = $~.captures
                # Substitute the capture index specified in the configuration for the corresponding
                # template variable, e.g. the value filter:
                #   /(Grade )?(\d)/ : $2
                # is parsed as (reval, ndxs) = (/(Grade )?(\d)/, 1) 
                # and transforms 'Grade 3' to cpts[0], or '3'.
                fmtd = reval % ndxs.map { |i| cpts[i] }
                fmtd unless fmtd.blank?
              end
            elsif defined? @catch_all then
              @catch_all
            else
              value
            end
          end
        end
      end
      
      # Parses the configuration pattern string => value hash into a regexp => value hash
      # qualified by the match indexes used to substitute match captures into the hash value.
      #
      # The pattern hash value can include match references ($1, $2, etc.). In that case,
      # the match captures substitute into a %s format reference in the result.
      #
      # @example
      #   regexp_hash({'/Golf/i' => 1}) #=> {1, []}
      #   regexp_hash({'/Hole (\d{1,2})/' => $1}) #=> {'%', [0]}
      #                        
      # @param [{String => Object}] pat_hash the string => value hash
      # @return [{Regexp => (Object, <Integer>)}] the corresponding regexp => (value, indexes) hash
      def regexp_hash(pat_hash)
        # The Regexp => value hash is built from the pattern => value hash.
        reh = {}
        # Make a matcher for each regexp pattern.
        pat_hash.each do |k, v|
          # The /pattern/opts string is parsed to the pattern and options.
          pat, opt = REGEXP_PAT.match(k).captures
          # the catch-all matcher
          if pat == '.*' then
            @catch_all = v
            next
          end
          # Convert the regexp i option character to a Regexp initializer parameter.
          reopt = if opt then
            case opt
              when 'i' then Regexp::IGNORECASE
              else Jinx.fail(MigrationError, "Migration value filter regular expression #{k} qualifier not supported: expected 'i', found '#{opt}'")
            end
          end
          # the Regexp object
          re = Regexp.new(pat, reopt)
          # Replace each $ match reference with a %s format reference.
          reh[re] = parse_regexp_value(v)
        end
        reh
      end

      # @example
      #   parse_regexp_value('Grade $2') #=> ['Grade %s', [1]]
      # @param value the value in the configuration regexp => value entry
      # @return (Object, <Integer>) the parsed (value, indexes) 
      # @see #regexp_hash
      def parse_regexp_value(value)
        return [value, Array::EMPTY_ARRAY] unless value =~ /\$\d/
        tmpl = value.gsub(/\$\d/, '%s')
        # Look for match references of the form $n.
        ndxs = value.scan(/\$(\d)/).map { |matches| matches.first.to_i - 1 }
        [tmpl, ndxs]
      end
    end
  end
end
