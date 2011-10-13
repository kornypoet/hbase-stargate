require File.dirname(__FILE__) + '/operation/meta_operation'
require File.dirname(__FILE__) + '/operation/table_operation'
require File.dirname(__FILE__) + '/operation/row_operation'
require File.dirname(__FILE__) + '/operation/scanner_operation'
require File.dirname(__FILE__) + '/middleware/net_http_client'

module Stargate
  class Client
    include Operation::MetaOperation
    include Operation::TableOperation
    include Operation::RowOperation
    include Operation::ScannerOperation

    attr_reader :url, :connection

    MIDDLEWARE = {
      "net_http" => NetHttpClient
    }

    def initialize(url = "http://localhost:8080", use = "net_http", opts = {})
      @url = URI.parse(url)
      unless @url.kind_of? URI::HTTP
        raise "invalid http url: #{url}"
      end
      @connection = MIDDLEWARE[use.to_s.downcase].new(@url, opts)
      # Not actually opening the connection yet, just setting up the persistent connection.
      # if opts[:proxy]
      #   proxy_address, proxy_port = opts[:proxy].split(':')
      #   @connection = Net::HTTP.Proxy(proxy_address, proxy_port).new(@url.host, @url.port)
      # else
      #   @connection = Net::HTTP.new(@url.host, @url.port)
      # end
      # @connection.read_timeout = opts[:timeout] if opts[:timeout]
    end

    def get(path, options = {})
      safe_request { @connection.get(:path => @url.path + path, :head => {"Accept" => "application/json", "Accept-Encoding" => "identity"}.merge(options)) }
    end

    def get_response(path, options = {})
      safe_response { @connection.get(:path => @url.path + path, :head => {"Accept" => "application/json", "Accept-Encoding" => "identity"}.merge(options)) }
    end

    def post(path, data = nil, options = {})
      safe_request { @connection.post(:path => @url.path + path, :body => data, :head => {'Content-Type' => 'text/xml', "Accept-Encoding" => "identity"}.merge(options)) }
    end

    def post_response(path, data = nil, options = {})
      safe_response { @connection.post(:path => @url.path + path, :body => data, :head => {'Content-Type' => 'text/xml', "Accept-Encoding" => "identity"}.merge(options)) }
    end

    def delete(path, options = {})
      safe_request { @connection.delete(:path => @url.path + path, :head => options) }
    end

    def delete_response(path, options = {})
      safe_response { @connection.delete(:path => @url.path + path, :head => options) }
    end

    def put(path, data = nil, options = {})
      safe_request { @connection.put(:path => @url.path + path, :body => data, :head => {'Content-Type' => 'text/xml', "Accept-Encoding" => "identity"}.merge(options)) }
    end

    def put_response(path, data = nil, options = {})
      safe_response { @connection.put(:path => @url.path + path, :body => data, :head => {'Content-Type' => 'text/xml', "Accept-Encoding" => "identity"}.merge(options)) }
    end

    private

      def safe_response(&block)
        begin
          yield
        rescue Errno::ECONNREFUSED
          raise ConnectionNotEstablishedError, "can't connect to #{@url}"
        rescue Timeout::Error => e
          puts e.backtrace.join("\n")
          raise ConnectionTimeoutError, "execution expired. Maybe query disabled tables"
        end
      end

      def safe_request(&block)
        response = safe_response{ yield block }
        if response.successful?
          response.body
        else
          response.create_error!
        end
        # case response
        # when Net::HTTPSuccess
        #   response.body
        # else
        #   response.error!
        # end
      end

  end
end
