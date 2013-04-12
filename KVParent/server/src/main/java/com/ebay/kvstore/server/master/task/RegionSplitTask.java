package com.ebay.kvstore.server.master.task;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.protocol.request.SplitRegionRequest;
import com.ebay.kvstore.server.master.engine.IMasterEngine;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public class RegionSplitTask extends LoadBalanceTask {

	protected Map<Integer, Integer> regionIds;

	public RegionSplitTask(IConfiguration conf, IMasterEngine engine) {
		super(conf, engine);
		this.interval = conf.getInt(IConfigurationKey.Master_Split_Check_Interval);
		this.regionIds = new HashMap<>();
	}

	@Override
	public void onRegionSplit(int oldId, int newId) {
		super.onRegionSplit(oldId, newId);
		regionIds.remove(oldId);
		regionIds.remove(newId);

	}

	@Override
	protected void process() {
		synchronized (engine) {
			Collection<DataServerStruct> dataServers = engine.getAllDataServers();
			Map<Integer, Address> targets = balancer.splitRegion(dataServers);
			if (targets != null) {
				for (Entry<Integer, Address> e : targets.entrySet()) {
					Integer nid = regionIds.get(e.getKey());
					if (nid == null) {
						nid = engine.nextRegionId();
						regionIds.put(e.getKey(), nid);
					}
					sendRequest(e.getValue(), new SplitRegionRequest(e.getKey(), nid));
				}
			}

		}
	}

}
