package com.ebay.kvstore.util;

import static org.junit.Assert.assertArrayEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import com.ebay.kvstore.structure.Region;

public class RegionUtilTest extends TestCase {

	private List<Region> regions;

	public RegionUtilTest() {
		regions = new ArrayList<>();
		regions.add(new Region(0, new byte[] { 1 }, new byte[] { 5 }));
		regions.add(new Region(0, new byte[] { 6 }, new byte[] { 10 }));
		regions.add(new Region(0, new byte[] { 1, 20 }, new byte[] { -50, 10 }));
		regions.add(new Region(0, new byte[] { 1, 1, 1 }, new byte[] { 10, 10, 10 }));
		Collections.sort(regions);
	}

	@Test
	public void testsearch() {

		Region r = RegionUtil.search(regions, new byte[] { 0 });
		assertEquals(true, r == null);

		r = RegionUtil.search(regions, new byte[] { 4 });
		assertArrayEquals(new byte[] { 1 }, r.getStart());
		assertArrayEquals(new byte[] { 5 }, r.getEnd());

		r = RegionUtil.search(regions, new byte[] { -60, 20 });
		assertArrayEquals(new byte[] { 1, 20 }, r.getStart());
		assertArrayEquals(new byte[] { -50, 10 }, r.getEnd());

		r = RegionUtil.search(regions, new byte[] { 5, 2, 4 });
		assertArrayEquals(new byte[] { 1, 1, 1 }, r.getStart());
		assertArrayEquals(new byte[] { 10, 10, 10 }, r.getEnd());

		r = RegionUtil.search(regions, new byte[] { 1, 1, 1, 1 });
		assertNull(r);
	}


}
