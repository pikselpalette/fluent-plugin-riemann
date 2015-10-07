require 'riemann/client'

class Fluent::RiemannOutput < Fluent::BufferedOutput
  class ConnectionFailure < StandardError; end
  Fluent::Plugin.register_output('riemann', self)

  config_param :host,     :string,  :default => '127.0.0.1'
  config_param :port,     :integer, :default => 5555
  config_param :timeout,  :integer, :default => 5
  config_param :ttl,      :integer, :default => 90
  config_param :protocol, :string,  :default => 'tcp'
  config_param :fields,   :hash,    :default => {}
  config_param :fields_from_metric, :string, :default => nil

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
    now = Time.now.to_i
    expiretime = Time.now.to_i - @ttl.to_i
    chunk.msgpack_each do |tag, time, record|
      record.each { |k, v|
        next unless v = remap(v)

        if time.to_i < expiretime
          log.warn "Dropping event, past the ttl."
          next
        end

        event = {
          :time    => time,
          :state   => 'ok',
          :ttl     => @ttl,
          :uptime  => IO.read('/proc/uptime').split[0].to_i,
          :metric  => v,
        }

        @fields.each { |f, i|
          event[f.to_sym] = i
        }

        if @fields_from_metric
          spots = k.split('.')
          flds = @fields_from_metric.split(',')
          flds.each_with_index do |f, i|
            event[f.to_sym] = spots[i]
          end
          if flds.include?('service')
            k = event[:service]
          else
            spots = spots[flds.length .. -1]
            k = spots.join('.')
          end
        end

        event[:service] = k.gsub(/\./, ' ')

        retries = 0
        begin
          client << event
        rescue Exception => e
          if retries < 2
            retries += 1
            log.warn "Could not push metrics to Riemann, resetting connection and trying again. #{e.message}"
            @_client = nil
            sleep 2**retries
            retry
          end
          raise ConnectionFailure, "Could not push metrics to Riemann after #{retries} retries. #{e.message}"
        end
      }
    end
  end
end
