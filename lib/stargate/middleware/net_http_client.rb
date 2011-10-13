require 'net/http'

module Stargate
  class NetHttpClient
 
    def initialize url, opts = {}
      if opts[:proxy]
        proxy_address, proxy_port = opts[:proxy].split(':')
        @connection = Net::HTTP.Proxy(proxy_address, proxy_port).new(url.host, url.port)
      else
        @connection = Net::HTTP.new(url.host, url.port)
      end
      @connection.read_timeout = opts[:timeout] if opts[:timeout]
    end

    def get(options={}) 
       @connection.get(options[:path], options[:head]) 
    end

    def post(options={})
      @connection.post(options[:path], options[:body], options[:head]) 
    end

    def delete(options={})
      @connection.delete(options[:path], options[:head]) 
    end
 
    def put(options={})
      @connection.put(options[:path], options[:body], options[:head]) 
    end

  end
end

class Net::HTTPResponse

  def successful?
    self.is_a? Net::HTTPSuccess
  end

  def content
    self.body
  end

  def create_error!
    self.error!
  end

end 
