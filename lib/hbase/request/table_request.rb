module HBase
  module Request
    class TableRequest < BasicRequest
      attr_reader :name
      attr_reader :body

      def initialize(name)
        super("")
        @name = CGI.escape(name) if name
      end

      def show
        @path << "#{name}"
      end

      def regions(start_row = nil, end_row = nil)
        @path << "/#{name}/regions"
      end

      def create
        @path << "/"
      end

      def enable
        @path << "/#{name}/enable"
      end

      def disable
        @path << "/#{name}/disable"
      end

      def delete
        @path << "/#{name}"
      end
    end
  end
end
