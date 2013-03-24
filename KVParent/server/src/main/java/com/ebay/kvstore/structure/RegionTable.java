package com.ebay.kvstore.structure;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;

import com.ebay.kvstore.Address;

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
	private Map<Region, Address> map;

	public RegionTable() {
		map = new Hashtable<>();
	}

	public void addRegion(Region region, Address addr) {
		map.put(region, addr);
	}

	public Address getRegionAddr(Region region) {
		return map.get(region);
	}

}
