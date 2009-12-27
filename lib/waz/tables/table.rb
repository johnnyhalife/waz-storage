module WAZ
  module Tables
    # This class represents a Table on Windows Azure Tables API. These are the methods implemented from Microsoft's API description 
    # available on MSDN at http://msdn.microsoft.com/en-us/library/dd179423.aspx
    #
    #	# list available tables
    #	WAZ::Tables::Table.list
    #
    #	# create a new table
    #	new_table = WAZ::Tables::Table.create('test-table')
    #
    #	# delete table
    #	new_table.destroy!
    #
    class Table
      class << self
        # Returns an array of the tables (WAZ::Tables::Table) existing on the current 
        # Windows Azure Storage account.
        def list()
          service_instance.list_tables().map do |table|
            WAZ::Tables::Table.new(table)
          end
        end
        
        # Creates a table on the current account.
        def create(table_name)
          raise WAZ::Storage::InvalidParameterValue, {:name => "name", :values => ["lower letters, numbers or - (hypen), and must not start or end with - (hyphen)"]} unless WAZ::Storage::ValidationRules.valid_name?(table_name)
          service_instance.create_table(table_name)
          WAZ::Tables::Table.new(:name => table_name)
        end
        
        # This method is internally used by this class. It's the way we keep a single instance of the 
        # service that wraps the calls the Windows Azure Tables API. It's initialized with the values
        # from the default_connection on WAZ::Storage::Base initialized thru establish_connection!
        def service_instance
          options = WAZ::Storage::Base.default_connection.merge(:type_of_service => "table")
          (@service_instances ||= {})[:account_name] ||= Service.new(options)
        end       
      end

      attr_accessor :name

      def initialize(options = {})
        raise WAZ::Storage::InvalidOption, :name unless options.keys.include?(:name)
        self.name = options[:name]
      end
      
      # Removes the table from the current account.
      def destroy!
        self.class.service_instance.delete_table(self.name)
      end
    end
  end
end