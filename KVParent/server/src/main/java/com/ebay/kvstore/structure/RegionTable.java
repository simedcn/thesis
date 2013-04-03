package com.ebay.kvstore.structure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import com.ebay.kvstore.RegionUtil;

/**
 * Contains each table and related addr
 * 
 * @author luochen
 * 
 */
public class RegionTable implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SortedMap<Region, Address> map;
	private List<Region> regions;
	private boolean sorted = false;

	public RegionTable() {
		map = new TreeMap<>();
		regions = new ArrayList<>();
	}

	public void addRegion(Region region, Address addr) {
		map.put(region, addr);
		regions.add(region);
	}

	public void clear() {
		regions.clear();
		map.clear();
	}

	public Address getKeyAddr(byte[] key) {
		Region region = getKeyRegion(key);
		if (region == null) {
			return null;
		} else {
			return map.get(region);
		}
	}

	public Region getKeyRegion(byte[] key) {
		if (!sorted) {
			Collections.sort(regions);
			sorted = true;
		}
		return RegionUtil.search(regions, key);
	}

	public Address getRegionAddr(Region region) {
		return map.get(region);
	}

}
