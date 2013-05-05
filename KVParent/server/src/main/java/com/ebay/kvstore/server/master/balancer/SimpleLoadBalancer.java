package com.ebay.kvstore.server.master.balancer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import com.ebay.kvstore.server.conf.IConfiguration;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public class SimpleLoadBalancer extends BaseLoadBalancer {

	public SimpleLoadBalancer(IConfiguration conf) {
		super(conf);
	}

	@Override
	public Map<Region, Address> assignRegion(Collection<Region> regions,
			Collection<DataServerStruct> dataServers) {
		if (dataServers == null || dataServers.size() == 0) {
			return null;
		}
		int regionLeft = regions.size();
		DataServerStruct[] structs = dataServers.toArray(new DataServerStruct[dataServers.size()]);
		Arrays.sort(structs, new DataServerComparator());
		int max = structs[structs.length - 1].getRegions().size();
		Iterator<Region> it = regions.iterator();
		for (DataServerStruct struct : structs) {
			int regionNum = struct.getRegions().size();
			int assign = regionLeft > (max - regionNum) ? (max - regionNum) : regionLeft;
			regionLeft -= assign;
			for (int i = 0; i < assign; i++) {
				Region region = it.next();
				if (!loadTargets.containsKey(region)) {
					loadTargets.put(region, struct.getAddr());
				}
			}
			if (regionLeft <= 0) {
				break;
			}
		}
		int i = 0;
		while (it.hasNext()) {
			Region region = it.next();
			if (!loadTargets.containsKey(region)) {
				loadTargets.put(region, structs[i++ % structs.length].getAddr());
			}
		}
		return Collections.unmodifiableMap(loadTargets);
	}

	@Override
	public Map<Integer, Address> unassignRegion(Collection<DataServerStruct> dataServers) {
		if (dataServers == null || dataServers.size() == 0) {
			return null;
		}
		DataServerStruct[] structs = dataServers.toArray(new DataServerStruct[dataServers.size()]);
		Arrays.sort(structs, new DataServerComparator());
		int total = 0;
		for (DataServerStruct struct : structs) {
			total += struct.getRegions().size();
		}
		int min = structs[0].getRegions().size();
		int average = total / structs.length;
		for (int i = structs.length - 1; i >= 0; i--) {
			int regionNum = structs[i].getRegions().size();
			if (regionNum > min * threshhold) {
				int balance = regionNum - average;
				Iterator<Region> it = structs[i].getRegions().iterator();
				for (int j = 0; j < balance; j++) {
					Region region = it.next();
					if (!isBusy(region)) {
						unloadTargets.put(region.getRegionId(), structs[i].getAddr());
					}
				}
			} else {
				break;
			}
		}
		return Collections.unmodifiableMap(unloadTargets);
	}

	private class DataServerComparator implements Comparator<DataServerStruct> {
		@Override
		public int compare(DataServerStruct ds1, DataServerStruct ds2) {
			return Integer.compare(ds1.getRegions().size(), ds2.getRegions().size());
		}

	}

}
