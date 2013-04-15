package com.ebay.kvstore.server.master.task;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.protocol.request.MergeRegionRequest;
import com.ebay.kvstore.server.master.balancer.RegionPair;
import com.ebay.kvstore.server.master.engine.IMasterEngine;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public class RegionMergeTask extends LoadBalanceTask {

	protected Map<RegionPair, Integer> regionIds;

	public RegionMergeTask(IConfiguration conf, IMasterEngine engine) {
		super(conf, engine);
		this.regionIds = new HashMap<>();
		this.interval = conf.getInt(IConfigurationKey.Master_Merge_Check_Interval);
	}

	@Override
	protected void process() {
		synchronized (engine) {
			Collection<DataServerStruct> dataServers = engine.getAllDataServers();
			Map<RegionPair, Address> targets = balancer.mergeRegion(dataServers);
			if (targets != null) {
				for (Entry<RegionPair, Address> e : targets.entrySet()) {
					RegionPair pair = e.getKey();
					Integer nid = regionIds.get(pair);
					if (nid == null) {
						nid = engine.nextRegionId();
						regionIds.put(pair, nid);
					}
					sendRequest(e.getValue(),
							new MergeRegionRequest(pair.getRegionId1(), pair.getRegionId2(), nid));
				}
			}

		}
	}

	@Override
	public void onRegionMerge(DataServerStruct struct, int regionId1, int regionId2) {
		super.onRegionMerge(struct, regionId1, regionId2);
		regionIds.remove(new RegionPair(regionId1, regionId2));
	}

}
