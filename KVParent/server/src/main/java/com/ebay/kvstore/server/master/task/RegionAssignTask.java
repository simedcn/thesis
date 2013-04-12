package com.ebay.kvstore.server.master.task;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.protocol.request.LoadRegionRequest;
import com.ebay.kvstore.server.master.engine.IMasterEngine;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

/**
 * Used for check unassigned region and choose proper data server to assign.
 * 
 * @author luochen
 * 
 */
public class RegionAssignTask extends LoadBalanceTask {
	// in milliseconds
	private static Logger logger = LoggerFactory.getLogger(RegionAssignTask.class);

	public RegionAssignTask(IConfiguration conf, IMasterEngine engine) {
		super(conf, engine);
		this.interval = this.conf.getInt(IConfigurationKey.Master_Assign_Check_Interval);
	}

	@Override
	protected final void process() {
		synchronized (engine) {
			Map<Integer, Region> unassigned = engine.getUnassignedRegions();
			if (unassigned.size() == 0) {
				return;
			}
			Collection<DataServerStruct> dataServers = engine.getAllDataServers();
			Collection<Region> regions = unassigned.values();
			Map<Region, Address> targets = balancer.assignRegion(regions, dataServers);
			if (targets != null) {
				for (Entry<Region, Address> e : targets.entrySet()) {
					sendRequest(e.getValue(), new LoadRegionRequest(e.getKey()));
				}
			}

		}
	}

}
