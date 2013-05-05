package com.ebay.kvstore.server.master.balancer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.ebay.kvstore.server.conf.IConfiguration;
import com.ebay.kvstore.server.conf.IConfigurationKey;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.util.KeyValueUtil;

public abstract class BaseLoadBalancer implements ILoadBalancer {

	protected Map<Region, Address> loadTargets;

	protected Map<Integer, Address> unloadTargets;

	protected Map<Integer, Address> splitTargets;

	protected Map<RegionPair, Address> mergeTargets;

	protected Set<Integer> mergeRegions;

	protected long regionMax;

	protected IConfiguration conf;

	protected int threshhold;

	public BaseLoadBalancer(IConfiguration conf) {
		this.loadTargets = new HashMap<>();
		this.unloadTargets = new HashMap<>();
		this.splitTargets = new HashMap<>();
		this.mergeRegions = new HashSet<>();
		this.mergeTargets = new HashMap<>();
		this.conf = conf;
		this.regionMax = conf.getLong(IConfigurationKey.Dataserver_Region_Max);
		this.threshhold = conf.getInt(IConfigurationKey.Master_Unassign_Threshhold);
	}

	@Override
	public synchronized Map<RegionPair, Address> mergeRegion(
			Collection<DataServerStruct> dataServers) {
		for (DataServerStruct struct : dataServers) {
			Collection<Region> regions = struct.getRegions();
			Address addr = struct.getAddr();
			Region[] regionArrays = regions.toArray(new Region[] {});
			for (int i = 0; i < regionArrays.length - 1; i++) {
				Region region1 = regionArrays[i];
				Region region2 = regionArrays[i + 1];
				if (mergable(region1, region2)) {
					mergeRegions.add(region1.getRegionId());
					mergeRegions.add(region2.getRegionId());
					mergeTargets.put(new RegionPair(region1.getRegionId(), region2.getRegionId()),
							addr);
					i++;
				}
			}
		}
		return mergeTargets;
	}

	@Override
	public synchronized void onDataServerUnload(DataServerStruct ds) {
		Address addr = ds.getAddr();
		removeAddress(loadTargets, addr);
		removeAddress(unloadTargets, addr);
		removeAddress(splitTargets, addr);
		removeMerge(addr);

	}

	@Override
	public synchronized void onRegionLoad(Region region) {
		loadTargets.remove(region);
	}

	@Override
	public synchronized void onRegionMerge(int regionId1, int regionId2) {
		mergeTargets.remove(new RegionPair(regionId1, regionId2));
		mergeRegions.remove(regionId1);
		mergeRegions.remove(regionId2);
	}

	@Override
	public synchronized void onRegionSplit(int oldId, int newId) {
		splitTargets.remove(oldId);
		splitTargets.remove(newId);
	}

	@Override
	public synchronized void onRegionUnload(int regionId) {
		unloadTargets.remove(regionId);
	}

	@Override
	public synchronized Map<Integer, Address> splitRegion(Collection<DataServerStruct> dataServers) {
		for (DataServerStruct struct : dataServers) {
			Collection<Region> regions = struct.getRegions();
			Address addr = struct.getAddr();
			for (Region region : regions) {
				if (region.getStat().size > regionMax && !isBusy(region)) {
					splitTargets.put(region.getRegionId(), addr);
				}
			}
		}
		return Collections.unmodifiableMap(splitTargets);
	}

	protected boolean isBusy(Region region) {
		int regionId = region.getRegionId();
		return loadTargets.containsKey(region) || unloadTargets.containsKey(regionId)
				|| splitTargets.containsKey(regionId) || mergeRegions.contains(regionId);
	}

	private boolean mergable(Region region1, Region region2) {
		if (region1.getStat().size + region2.getStat().size < regionMax / 2) {
			byte[] key1 = region1.getEnd();
			byte[] key2 = region2.getStart();
			if (Arrays.equals(KeyValueUtil.nextKey(key1), key2) && !isBusy(region1)
					&& !isBusy(region2)) {
				return true;
			}
		}
		return false;
	}

	@SuppressWarnings("rawtypes")
	private synchronized void removeAddress(Map targets, Address addr) {
		Iterator it = targets.entrySet().iterator();
		while (it.hasNext()) {
			Entry e = (Entry) it.next();
			if (e.getValue().equals(addr)) {
				it.remove();
			}
		}
	}

	private void removeMerge(Address addr) {
		Iterator<Entry<RegionPair, Address>> it = mergeTargets.entrySet().iterator();
		while (it.hasNext()) {
			Entry<RegionPair, Address> e = it.next();
			if (e.getValue().equals(addr)) {
				RegionPair pair = e.getKey();
				mergeRegions.remove(new Region(pair.getRegionId1(), null, null));
				mergeRegions.remove(new Region(pair.getRegionId2(), null, null));
				it.remove();
			}
		}
	}
}
