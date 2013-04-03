package com.ebay.kvstore.server.master.task;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.server.master.balancer.ILoadBalancer;
import com.ebay.kvstore.server.master.balancer.LoadBalancerFactory;
import com.ebay.kvstore.server.master.helper.IMasterEngine;
import com.ebay.kvstore.server.master.helper.IMasterEngineListener;
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
	public void onRegionSplit(Region oldRegion, Region newRegion) {
		balancer.onRegionSplit(oldRegion, newRegion);
	}

	@Override
	public void onRegionUnload(DataServerStruct struct, Region region) {
		balancer.onRegionUnload(region);
	}

}
