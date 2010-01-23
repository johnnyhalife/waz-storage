module WAZ
  module Tables
    class TableArray << Array
      attr_accessor :continuation_token
    end
  end
end