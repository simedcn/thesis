package com.ebay.kvstore.server.master.balancer;

public class RegionPair {

	private int regionId1;

	private int regionId2;

	public RegionPair(int regionId1, int regionId2) {
		super();
		this.regionId1 = regionId1;
		this.regionId2 = regionId2;
	}

	public int getRegionId1() {
		return regionId1;
	}

	public void setRegionId1(int regionId1) {
		this.regionId1 = regionId1;
	}

	public int getRegionId2() {
		return regionId2;
	}

	public void setRegionId2(int regionId2) {
		this.regionId2 = regionId2;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + regionId1 * regionId2;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RegionPair other = (RegionPair) obj;
		if (regionId1 != other.regionId1 && regionId1 != other.regionId2)
			return false;
		if (regionId2 != other.regionId2 && regionId2 != other.regionId1)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "RegionPair [regionId1=" + regionId1 + ", regionId2=" + regionId2 + "]";
	}

}
