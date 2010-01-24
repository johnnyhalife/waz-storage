module WAZ
  module Tables
    class EdmTypeHelper
      class << self
        def parse_from(item)
          return nil if !item.attributes['m:null'].nil? and item.attributes['m:null'] == 'true'
          case item.attributes['m:type']
            when 'Edm.Int16', 'Edm.Int32', 'Edm.Int64'
              item.text.to_i
            when 'Edm.Single', 'Edm.Double'
              item.text.to_f
            when 'Edm.Boolean'
              item.text == 'true'
            when 'Edm.DateTime'
              Time.parse(item.text)
            when 'Edm.Binary'
              StringIO.new(Base64.decode64(item.text))
            else
              item.text
          end
        end
      
        def parse_to(item)
          case item.class.name
            when 'String'
              [item, 'Edm.String']
            when 'Fixnum'
              [item, 'Edm.Int32']
            when 'Float'
              [item, 'Edm.Double']
            when 'TrueClass', 'FalseClass'          
              [item, 'Edm.Boolean']
            when 'Time'
              [item.iso8601, 'Edm.DateTime']
            when 'File', 'StringIO'
              item.pos = 0
              [Base64.encode64(item.read), 'Edm.Binary']
            else
              [item, 'Edm.String']
          end
        end
      end
    end
  end
end