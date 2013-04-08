package com.ebay.kvstore.server.master.helper;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.exception.InvalidDataServerException;
import com.ebay.kvstore.exception.InvalidRegionException;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.server.master.task.IMasterTask;
import com.ebay.kvstore.structure.Address;
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

	public boolean containsDataClient(Address addr);

	public boolean containsDataServer(Address addr);

	public Collection<DataServerStruct> getAllDataServers();

	public DataServerStruct getDataServer(Address addr);

	public DataServerStruct getDataServer(Region region) throws InvalidRegionException;

	public DataServerStruct getDataServerByClient(Address addr) throws InvalidDataServerException;

	public IoSession getDataServerConnection(Address addr) throws InvalidDataServerException;

	public IoSession getDataServerConnectionByClient(Address addr)
			throws InvalidDataServerException;

	public RegionTable getRegionTable();

	public Map<Integer, Region> getUnassignedRegions();

	public void loadRegion(DataServerStruct struct, Region region)
			throws InvalidDataServerException;

	public int nextRegionId();

	public void registerListener(IMasterEngineListener listener);

	public void registerTask(IMasterTask task, boolean listener);

	public void removeDataServer(IoSession session);

	public void resetRegionId() throws Exception;

	public void setLogger(String path);

	public void splitRegion(DataServerStruct struct, Region oldRegion, Region newRegion)
			throws InvalidDataServerException;

	public void start() throws Exception;

	public void stop() throws IOException;

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
	public void syncDataServer(DataServerStruct struct, IoSession session) throws KVException;

	public void unloadRegoin(DataServerStruct struct, Region region)
			throws InvalidDataServerException;

	public void unregisterListener(IMasterEngineListener listener);

	public void unregisterTask(IMasterTask task);

	public boolean containsRegion(int regionId);

}
