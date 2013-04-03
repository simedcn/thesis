package com.ebay.kvstore.server.master.balancer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public abstract class BaseLoadBalancer implements ILoadBalancer {

	protected Map<Region, Address> loadTargets;

	protected Map<Region, Address> unloadTargets;

	protected Map<Region, Address> splitTargets;

	protected long regionMax;

	protected IConfiguration conf;

	protected int threshhold;

	public BaseLoadBalancer(IConfiguration conf) {
		this.loadTargets = new HashMap<>();
		this.unloadTargets = new HashMap<>();
		this.splitTargets = new HashMap<>();
		this.conf = conf;
		this.regionMax = conf.getLong(IConfigurationKey.Region_Max);
		this.threshhold = conf.getInt(IConfigurationKey.Master_Unassign_Threshhold);
	}

	@Override
	public void onDataServerUnload(DataServerStruct ds) {
		Address addr = ds.getAddr();
		removeAddress(loadTargets, addr);
		removeAddress(unloadTargets, addr);
		removeAddress(splitTargets, addr);
	}

	@Override
	public void onRegionLoad(Region region) {
		loadTargets.remove(region);
	}

	@Override
	public void onRegionSplit(Region oldRegion, Region newRegion) {
		splitTargets.remove(oldRegion);
		splitTargets.remove(newRegion);
	}

	@Override
	public void onRegionUnload(Region region) {
		unloadTargets.remove(region);
	}

	@Override
	public Map<Region, Address> splitRegion(Collection<DataServerStruct> dataServers) {
		for (DataServerStruct struct : dataServers) {
			Collection<Region> regions = struct.getRegions();
			Address addr = struct.getAddr();
			for (Region region : regions) {
				if (region.getStat().size > regionMax) {
					splitTargets.put(region, addr);
				}
			}
		}
		return splitTargets;
	}

	private void removeAddress(Map<Region, Address> targets, Address addr) {
		Iterator<Entry<Region, Address>> it = targets.entrySet().iterator();
		while (it.hasNext()) {
			Entry<Region, Address> e = it.next();
			if (e.getValue().equals(addr)) {
				it.remove();
			}
		}
	}
}
