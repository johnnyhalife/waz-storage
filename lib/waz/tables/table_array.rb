module WAZ
  module Tables
    class TableArray < Array
      attr_accessor :continuation_token
      
      def initialize(array)
        super(array)
      end
    end
  end
end