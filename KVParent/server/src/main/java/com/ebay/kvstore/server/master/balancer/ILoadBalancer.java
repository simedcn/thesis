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

	public void onDataServerUnload(DataServerStruct ds);

	public void onRegionLoad(Region region);

	public void onRegionSplit(Region oldRegion, Region newRegion);

	public void onRegionUnload(Region region);

	public Map<Region, Address> splitRegion(Collection<DataServerStruct> dataServers);

	public Map<Region, Address> unassignRegion(Collection<DataServerStruct> dataServers);

}
