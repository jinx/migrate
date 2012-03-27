module Jinx
  module Migration
    # A prototypical source reader which enumerates the input records.
    module Reader
      include Enumerable
  
      # @param [String] name the migration mapping source field name, e.g. +First Name+
      # @return [Symbol] the record value accessor symbol, e.g. +:first_name+
      def accessor(name); end
  
      # @yield [rec] migrate the source record
      # @yieldparam [{Symbol => Object}] rec the source accessor => value record
      def each; end
    end
  end
end
