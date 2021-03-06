package com.ebay.kvstore.server.master.task;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.ebay.kvstore.protocol.request.UnloadRegionRequest;
import com.ebay.kvstore.server.conf.IConfiguration;
import com.ebay.kvstore.server.conf.IConfigurationKey;
import com.ebay.kvstore.server.master.engine.IMasterEngine;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public class RegionUnassignTask extends LoadBalanceTask {

	protected Map<Region, Address> targets;

	public RegionUnassignTask(IConfiguration conf, IMasterEngine engine) {
		super(conf, engine);
		this.interval = conf.getInt(IConfigurationKey.Master_Unassign_Check_Interval);
		this.targets = new HashMap<>();
	}

	@Override
	protected void process() {
		synchronized (engine) {
			Collection<DataServerStruct> dataServers = engine.getAllDataServers();
			Map<Integer, Address> targets = balancer.unassignRegion(dataServers);
			if (targets != null) {
				for (Entry<Integer, Address> e : targets.entrySet()) {
					sendRequest(e.getValue(), new UnloadRegionRequest(e.getKey()));
				}
			}
		}
	}

}
