module Stargate
  module Response
    class ScannerResponse < BasicResponse
      attr_reader :method

      def initialize(raw_data, method)
        @method = method
        super(raw_data)
      end

      def parse_content(raw_data)
        case @method
        when :open_scanner
          # case raw_data
          # when Net::HTTPCreated
          if raw_data.successful?
            Stargate::Model::Scanner.new(:scanner_url => raw_data["Location"])
          else
            if raw_data.content.include?("TableNotFoundException")
              raise TableNotFoundError, "Table #{table_name} Not Found!"
            else
              raise StandardError, "Unable to open scanner. Received the following message: #{raw_data.content}"
            end
          end
        when :get_rows
          # Dispatch it to RowResponse, since that method is made
          # to deal with rows already.
          RowResponse.new(raw_data, :show_row).parse
        when :close_scanner
          # case raw_data
          # when Net::HTTPOK
          if raw_data.successful?
            return true
          else
            raise StandardError, "Unable to close scanner. Received the following message: #{raw_data.content}"
          end
        else
          puts "method '#{@method}' not supported yet"
        end
      end
    end
  end
end
