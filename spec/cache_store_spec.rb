require File.expand_path("spec_helper", File.dirname(__FILE__))

describe Riak::CacheStore do
  it "should be an implementation of ActiveSupport::Cache::Store" do
    Riak::CacheStore.should < ActiveSupport::Cache::Store
  end

  it "should work with lookup" do
    ActiveSupport::Cache.lookup_store(:riak_store).should be_instance_of(Riak::CacheStore)
  end
  
  describe "when initializing" do
    before :each do
      
    end
    
    it "should ensure that the bucket does not allow divergence" do
    
    end
  end
  
end
