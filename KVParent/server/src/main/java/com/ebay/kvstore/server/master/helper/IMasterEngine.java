package com.ebay.kvstore.server.master.helper;

import java.io.IOException;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionTable;

/**
 * Used for control data server and regions in master server.
 * 
 * @author luochen
 * 
 */
public interface IMasterEngine {

	public void addDataServer(DataServerStruct struct, IoSession session);

	/**
	 * Assign region to a data server. Will send a LoadRegionRequest to server.
	 * When the return code shows success, the
	 * {@link IMasterEngine#loadRegion(Address, Region)} will be called.
	 * 
	 * @param struct
	 * @param reigon
	 */
	public void assignRegion(DataServerStruct struct, Region reigon);

	/**
	 * check all regions whether they exceeds the region limit size
	 */
	public void checkSplitRegion();

	public DataServerStruct[] getAllDataServers();

	public DataServerStruct getDataServer(Address addr);

	public DataServerStruct getDataServer(Region region);

	/**
	 * Used for sync regions, usually called at the startup phase of the
	 * cluster. 1. if the region contained in the struct has not be assigned,
	 * then the data server will serve these regions. 2.Or if the region has
	 * been assigned before, but not this server, then this data server should
	 * unload the region. 3. Or if the region does not exist in master
	 * server(rarely happens), then this region will also be served by this data
	 * server
	 * 
	 * @param struct
	 * @param region
	 */

	public RegionTable getRegionTable();

	public void loadRegion(Address addr, Region region);

	public int nextRegionId() throws Exception;

	public void removeDataServer(Address addr);

	public void resetRegionId() throws Exception;

	public void shutdown() throws IOException;

	public void splitRegion(Address addr, Region oldRegion, Region newRegion);

	public void start() throws Exception;

	public void syncDataServer(DataServerStruct struct, IoSession session);

	/**
	 * Unassign region from a data server.
	 * 
	 * @param struct
	 * @param region
	 */
	public void unassignRegion(DataServerStruct struct, Region region);

	public void unloadRegoin(Address addr, Region region);

}
