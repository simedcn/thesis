package com.ebay.kvstore.server.master.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public class SimpleLoadBalancerTest extends BaseLoadBalancerTest {

	private ILoadBalancer loadBalancer;

	public SimpleLoadBalancerTest() {
		conf.set(IConfigurationKey.Master_Unassign_Threshhold, 2);
		loadBalancer = LoadBalancerFactory.createLoadBalancer(conf);
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testAssignRegion() {
		List<Region> unassignedRegions = new ArrayList<>();
		unassignedRegions.add(new Region(nextRegionId(), nextKeyRange(), nextKeyRange()));
		unassignedRegions.add(new Region(nextRegionId(), nextKeyRange(), nextKeyRange()));
		Map<Region, Address> result = loadBalancer.assignRegion(unassignedRegions, dataServers);
		for (Address addr : result.values()) {
			assertEquals(addr, addr3);
		}
	}

	@Test
	public void test() {
		for (int i = 0; i < 20; i++) {
			List<Region> unassignedRegions = new ArrayList<>();
			unassignedRegions.add(new Region(nextRegionId(), nextKeyRange(), nextKeyRange()));
			unassignedRegions.add(new Region(nextRegionId(), nextKeyRange(), nextKeyRange()));
			Map<Region, Address> result = loadBalancer.assignRegion(unassignedRegions, dataServers);
			for (Region region : result.keySet()) {
				Address addr = result.get(region);
				for (DataServerStruct ds : dataServers) {
					if (ds.getAddr().equals(addr)) {
						ds.addRegion(region);
						break;
					}
				}
			}
			for (Region region : unassignedRegions) {
				loadBalancer.onRegionLoad(region);
			}
		}
		for (DataServerStruct ds : dataServers) {
			System.out.println(ds.getRegions().size());
		}
	}

	@Test
	public void testUnassignRegion() {
		Map<Region, Address> result = loadBalancer.unassignRegion(dataServers);
		assertTrue(result.values().contains(addr2));
	}

	@Test
	public void testSplitRegion() {
		int regionId = nextRegionId();
		Region region = new Region(regionId, nextKeyRange(), nextKeyRange());
		region.getStat().size = 101 * 1024 * 1024;
		dataServers.get(2).addRegion(region);
		Map<Region, Address> result = loadBalancer.splitRegion(dataServers);
		assertEquals(1, result.size());
		assertTrue(result.containsKey(region));
	}

}
