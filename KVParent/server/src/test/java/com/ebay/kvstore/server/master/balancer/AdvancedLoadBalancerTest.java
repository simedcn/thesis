package com.ebay.kvstore.server.master.balancer;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.Region;

public class AdvancedLoadBalancerTest extends BaseLoadBalancerTest {

	public AdvancedLoadBalancerTest() {
		conf.set(IConfigurationKey.Master_Loadbalance_Policy, "advanced");
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
		for (Entry<Region, Address> e : result.entrySet()) {
			System.out.println(e.getKey() + ":" + e.getValue());
		}
	}

	@Test
	public void testUnassignRegion() {
		int regionId = nextRegionId();
		Region region = new Region(regionId, nextKeyRange(), nextKeyRange());
		region.getStat().size = 100;
		dataServers.get(2).addRegion(region);
		Map<Integer, Address> result = loadBalancer.unassignRegion(dataServers);
		assertEquals(2, result.size());
		assertTrue(result.containsKey(region));
	}

}
