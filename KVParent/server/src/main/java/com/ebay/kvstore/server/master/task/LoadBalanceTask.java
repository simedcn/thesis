package com.ebay.kvstore.server.master.task;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.server.master.balancer.ILoadBalancer;
import com.ebay.kvstore.server.master.balancer.LoadBalancerFactory;
import com.ebay.kvstore.server.master.engine.IMasterEngine;
import com.ebay.kvstore.server.master.engine.IMasterEngineListener;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public abstract class LoadBalanceTask extends BaseMasterTask implements IMasterEngineListener {

	protected ILoadBalancer balancer;

	public LoadBalanceTask(IConfiguration conf, IMasterEngine engine) {
		super(conf, engine);
		this.balancer = LoadBalancerFactory.createLoadBalancer(conf);
	}

	@Override
	public void onDataServerUnload(DataServerStruct struct) {
		balancer.onDataServerUnload(struct);
	}

	@Override
	public void onRegionLoad(DataServerStruct struct, Region region) {
		balancer.onRegionLoad(region);
	}

	@Override
	public void onRegionSplit(int oldId, int newId) {
		balancer.onRegionSplit(oldId, newId);
	}

	@Override
	public void onRegionUnload(DataServerStruct struct, int regionId) {
		balancer.onRegionUnload(regionId);
	}

	@Override
	public void onRegionMerge(DataServerStruct struct, int regionId1, int regionId2) {
		balancer.onRegionMerge(regionId1, regionId2);
	}

}
