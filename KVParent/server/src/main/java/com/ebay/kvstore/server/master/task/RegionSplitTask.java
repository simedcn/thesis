package com.ebay.kvstore.server.master.task;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.protocol.request.SplitRegionRequest;
import com.ebay.kvstore.server.master.helper.IMasterEngine;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public class RegionSplitTask extends LoadBalanceTask {

	protected Map<Integer, Integer> regionIds;

	public RegionSplitTask(IConfiguration conf, IMasterEngine engine) {
		super(conf, engine);
		this.interval = conf.getInt(IConfigurationKey.Master_Split_CheckInterval);
		this.regionIds = new HashMap<>();
	}

	@Override
	public void onRegionSplit(Region oldRegion, Region newRegion) {
		super.onRegionSplit(oldRegion, newRegion);
		regionIds.remove(oldRegion.getRegionId());
		regionIds.remove(newRegion.getRegionId());

	}

	@Override
	protected void process() {
		synchronized (engine) {
			Collection<DataServerStruct> dataServers = engine.getAllDataServers();
			Map<Region, Address> targets = balancer.splitRegion(dataServers);
			for (Entry<Region, Address> e : targets.entrySet()) {
				Integer nid = regionIds.get(e.getKey().getRegionId());
				if (nid == null) {
					nid = engine.nextRegionId();
					regionIds.put(e.getKey().getRegionId(), nid);
				}
				sendRequest(e.getValue(), new SplitRegionRequest(e.getKey().getRegionId(), nid));
			}
		}
	}

}
