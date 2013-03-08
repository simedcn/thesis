package com.ebay.chluo.kvstore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.*;

import junit.framework.TestCase;

import com.ebay.chluo.kvstore.structure.Region;

public class RegionUtilTest extends TestCase {

	private List<Region> regions;

	public RegionUtilTest() {
		regions = new ArrayList<>();
		regions.add(new Region(0, new byte[] { 1 }, new byte[] { 5 }, null));
		regions.add(new Region(0, new byte[] { 6 }, new byte[] { 10 }, null));
		regions.add(new Region(0, new byte[] { 1, 20 }, new byte[] { -50, 10 }, null));
		regions.add(new Region(0, new byte[] { 1, 1, 1 }, new byte[] { 10, 10, 10 }, null));
		Collections.sort(regions);
	}

	@Test
	public void testSearchRegion() {

		Region r = RegionUtil.searchRegion(regions, new byte[] { 0 });
		assertEquals(true, r == null);

		r = RegionUtil.searchRegion(regions, new byte[] { 4 });
		assertArrayEquals(new byte[] { 1 }, r.getStart());
		assertArrayEquals(new byte[] { 5 }, r.getEnd());

		r = RegionUtil.searchRegion(regions, new byte[] { -60, 20 });
		assertArrayEquals(new byte[] { 1, 20 }, r.getStart());
		assertArrayEquals(new byte[] { -50, 10 }, r.getEnd());

		r = RegionUtil.searchRegion(regions, new byte[] { 5,2,4 });
		assertArrayEquals(new byte[] {  1, 1, 1 }, r.getStart());
		assertArrayEquals(new byte[] { 10, 10, 10}, r.getEnd());

		r = RegionUtil.searchRegion(regions, new byte[] { 1, 1, 1, 1 });
		assertNull(r);
	}
}
