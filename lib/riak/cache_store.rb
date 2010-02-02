# Copyright 2010 Sean Cribbs, Sonian Inc., and Basho Technologies, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
require 'riak'

module Riak
  # Implements an ActiveSupport::Cache::Store on top of Riak.
  class CacheStore < ActiveSupport::Cache::Store
    attr_accessor :client, :bucket
    
    def initialize(options={})
      bucket_name = options.delete(:bucket) || "_cache"
      client = options.delete[:client]
      @client = client || Client.new(options)
      @bucket = @client.bucket(bucket_name, :keys => false)
      ensure_allow_mult_false

      extend ActiveSupport::Cache::Strategy::LocalCache
    end

    def read(key, options = nil)
      if rails3?
        super { real_read(key, options) }
      else
        super; real_read(key, options)
      end
    end

    def write(key, value, options = nil)
      if rails3?
        super { real_write(key, value, options) }
      else
        super; real_write(key, value, options)
      end
    end

    def delete(key, options = nil)
      if rails3?
        super { real_delete(key, options) }
      else
        super; real_delete(key, options)
      end
    end
    
    def delete_matched(matcher, options=nil)
      if rails3?
        super { real_delete_matched(matcher, options) }
      else
        super; real_delete_matched(matcher, options)
      end
    end
    
    def exist?(key, options=nil)
      if rails3?
        super { real_exist?(key, options) }
      else
        super; real_exist?(key, options)
      end
    end
    
    private
    def rails3?
      require 'active_support/version'
      ActiveSupport::VERSION::MAJOR >= 3
    end
    
    def ensure_allow_mult_false
      if @bucket.props['allow_mult']
        @bucket.props = @bucket.props.merge('allow_mult' => false)
      end
    end

    def escape_key(string)
      URI.escape(URI.escape(string), "/")
    end

    def expired?(object, options=nil)
      expires = object.meta['expires-at'] && Time.httpdate(object.meta['expires-at'])
      expires_in = expires_in(options)
      # These calculations assume your system clocks are reasonably synchronized
      case
      when expires
        expires < Time.now
      when expires_in > 0
        object.last_modified < Time.now - expires_in
      else
        false
      end
    end

    def real_read(key, options = nil)
      object = @bucket[escape_key(key), {:r => 1}]
      object.delete and return nil if expired?(object, options)
      object.data
    rescue FailedRequest => fr
      logger.error("Riak::CacheStore error: #{fr.inspect}")
      nil
    end

    def real_write(key, value, options=nil)
      object = Riak::RObject.new(@bucket, escape_key(key))
      expiration = expires_in(options)
      object.meta['expires-at'] = (Time.now + expiration).httpdate if expiration > 0
      object.meta["ruby-serialization"] = "Marshal"
      object.content_type = "application/octet-stream"
      object.store :returnbody => false, :r => 1, :w => 1
    rescue FailedRequest => fr
      logger.error("Riak::CacheStore error: #{fr.inspect}")
      false
    end

    def real_delete(key, options=nil)
      object = Riak::RObject.new(@bucket, escape_key(key))
      object.delete
    rescue FailedRequest => fr
      logger.error("Riak::CacheStore error: #{fr.inspect}")
      false
    end

    def real_delete_matched(matcher, options=nil)
      @bucket.keys do |key|
        next unless URI.unescape(key) =~ matcher
        begin
          @bucket[key].delete
        rescue FailedRequest => fr
          logger.error("Riak::CacheStore error: #{fr.inspect}")
        end
      end
    end

    def real_exist?(key, options=nil)
      @bucket[escape_key(key), {:r => 1}]
      true
    rescue FailedRequest => fr
      logger.error("Riak::CacheStore error: #{fr.inspect}") unless fr.code.to_i == 404
      false
    end
  end
end

ActiveSupport::Cache::RiakStore = Riak::CacheStore unless ActiveSupport::Cache.const_defined?(:RiakStore)
