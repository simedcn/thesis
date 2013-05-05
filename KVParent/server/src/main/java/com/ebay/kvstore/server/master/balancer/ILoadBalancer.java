package com.ebay.kvstore.server.master.balancer;

import java.util.Collection;
import java.util.Map;

import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

/**
 * 
 * @author luochen
 * 
 */
public interface ILoadBalancer {

	public Map<Region, Address> assignRegion(Collection<Region> regions,
			Collection<DataServerStruct> dataServers);

	public Map<RegionPair, Address> mergeRegion(Collection<DataServerStruct> dataServers);

	public void onDataServerUnload(DataServerStruct ds);

	public void onRegionLoad(Region region);

	public void onRegionMerge(int regionId1, int regionId2);

	public void onRegionSplit(int oldId, int newId);

	public void onRegionUnload(int regionId);

	public Map<Integer, Address> splitRegion(Collection<DataServerStruct> dataServers);

	public Map<Integer, Address> unassignRegion(Collection<DataServerStruct> dataServers);

}
