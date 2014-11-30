require 'riemann/client'

class Fluent::RiemannOutput < Fluent::BufferedOutput
  Fluent::Plugin.register_output('riemann', self)

  config_param :host,     :string,  :default => '127.0.0.1'
  config_param :port,     :integer, :default => 5555
  config_param :timeout,  :integer, :default => 5
  config_param :protocol, :string,  :default => 'tcp'
  config_param :fields,   :hash,    :default => {}
  config_param :fields_from_metric,  :bool, :default => false

  def initialize
    super
  end

  def configure(c)
    super

  end

  def start
    super
  end

  def shutdown
    super
  end

  def client
    @_client ||= Riemann::Client.new :host => @host, :port => @port, :timeout => @timeout
    @protocol == 'tcp' ? @_client.tcp : @_client.udp
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def remap(data)
    if data.is_a? String
      if data =~ /^\d+\.\d+$/
        data = data.to_f
      elsif data =~ /^\d+$/
        data = data.to_i
      else
        data = nil
      end
    end
    data
  end

  def write(chunk)
    chunk.msgpack_each do |tag, time, record|
      event = {
        :time    => time,
      }
      @fields.each { |k, v|
        event[k.to_sym] = v
      }
      record.each { |k, v|
        next unless v = remap(v)
        if @field_from_metric
          spots = k.split('.')
          ['stage', 'stack', 'app', 'instance'].each_with_index do |k, i|
            event[k] = spots[i]
          end
        end
        event[:service] = k.gsub(/\./, ' ')
        event[:metric] = v
        client << event
      }
    end
  end
end
