package com.ebay.kvstore.server.master.balancer;

import java.util.ArrayList;

import java.util.List;

import org.junit.Before;

import com.ebay.kvstore.server.util.BaseTest;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public abstract class BaseLoadBalancerTest extends BaseTest {

	protected List<DataServerStruct> dataServers;

	protected Address addr1;

	protected Address addr2;

	protected Address addr3;

	protected ILoadBalancer loadBalancer;

	protected static int regionId = 0;

	protected static byte key = 0;

	public BaseLoadBalancerTest() {
		addr1 = new Address("127.0.0.1", 30001);
		addr2 = new Address("127.0.0.1", 30002);
		addr3 = new Address("127.0.0.1", 30003);

	}

	@Before
	public void init() {
		regionId = 0;
		dataServers = new ArrayList<>();
		DataServerStruct server = new DataServerStruct(addr1, 1);
		server.addRegion(new Region(nextRegionId(), nextKeyRange(), nextKeyRange()));
		server.addRegion(new Region(nextRegionId(), nextKeyRange(), nextKeyRange()));
		dataServers.add(server);
		server = new DataServerStruct(addr2, 2);
		server.addRegion(new Region(nextRegionId(), nextKeyRange(), nextKeyRange()));
		server.addRegion(new Region(nextRegionId(), nextKeyRange(), nextKeyRange()));
		server.addRegion(new Region(nextRegionId(), nextKeyRange(), nextKeyRange()));
		dataServers.add(server);
		server = new DataServerStruct(addr3, 3);
		server.addRegion(new Region(nextRegionId(), nextKeyRange(), nextKeyRange()));
		dataServers.add(server);
	}

	public static int nextRegionId() {
		return regionId++;
	}

	public static byte[] nextKeyRange() {
		return new byte[] { key++ };
	}

}
