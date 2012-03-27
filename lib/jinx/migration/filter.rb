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
    #   /(\d{1,2})\/x\/(\d{1,2})/ : $1/1/$2
    #   n/a : ~
    # converts the input value as follows:
    #   3/12/02 => 3/12/02 (no match)
    #   5/x/04 => 5/1/04
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
      # @yield [input] converts the input field value into a caTissue property value
      # @yieldparam input the CSV input value 
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
      def to_proc(spec=nil)
        # Split the filter spec into a straight value => value hash and a pattern => value hash.
        ph, vh = spec.split { |k, v| k =~ REGEXP_PAT }
        # The Regexp => value hash is built from the pattern => value hash.
        reh = {}
        # Make a matcher for each regexp pattern.
        ph.each do |k, v|
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
          # The regexp value can include match references ($1, $2, etc.). In that case, replace the $
          # match reference with a %s print reference, since the filter formats the matching input value.
          reh[re] = String === v ? v.gsub(/\$\d/, '%s') : v
        end
        
        # The new proc matches preferentially on the literal value, then the first matching regexp.
        # If no match on either a literal or a regexp, then the value is preserved.
        Proc.new do |input|
          value = block_given? yield(input) : input
          if vh.has_key?(value) then
            vh[value]
          else
            # The first regex which matches the value.
            regexp = reh.detect_key { |re| value =~ re }
            # If there is a match, then apply the filter to the match data.
            # Otherwise, pass the value through unmodified.
            if regexp then
              v = reh[regexp]
              String === v ? v % $~.captures : v
            elsif defined? @catch_all then
              @catch_all
            else
              value
            end
          end
        end
      end
    end
  end
end
