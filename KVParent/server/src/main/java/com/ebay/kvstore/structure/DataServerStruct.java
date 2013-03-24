package com.ebay.kvstore.structure;

import java.io.Serializable;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import com.ebay.kvstore.Address;

public class DataServerStruct implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Address addr;

	private int weight;

	private SystemInfo info;

	private SortedSet<Region> regions;

	public DataServerStruct(Address addr, int weight) {
		this(addr, weight, new TreeSet<Region>());
	}

	public DataServerStruct(Address addr, int weight, SortedSet<Region> regions) {
		super();
		this.addr = addr;
		this.weight = weight;
		this.regions = regions;
		this.info = new SystemInfo();
	}

	public void addRegion(Region region) {
		regions.add(region);
	}

	public void addRegions(Collection<Region> regions) {
		this.regions.addAll(regions);
	}

	public boolean containsRegion(Region r) {
		return regions.contains(r);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataServerStruct other = (DataServerStruct) obj;
		if (addr == null) {
			if (other.addr != null)
				return false;
		} else if (!addr.equals(other.addr))
			return false;
		return true;
	}

	public Address getAddr() {
		return addr;
	}

	public Region[] getAllRegions() {
		return regions.toArray(new Region[] {});
	}

	public SystemInfo getInfo() {
		return info;
	}

	public SortedSet<Region> getRegions() {
		return regions;
	}

	public int getWeight() {
		return weight;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((addr == null) ? 0 : addr.hashCode());
		return result;
	}

	public void removeRegion(Region region) {
		regions.remove(region);
	}

	public void setAddr(Address addr) {
		this.addr = addr;
	}

	public void setInfo(SystemInfo info) {
		this.info = info;
	}

	public void setRegions(SortedSet<Region> regions) {
		this.regions = regions;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public void updateSystemInfo() {
		this.info.update();
	}
}
