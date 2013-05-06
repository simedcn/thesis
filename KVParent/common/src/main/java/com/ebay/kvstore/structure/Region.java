package com.ebay.kvstore.structure;

import java.io.Serializable;
import java.util.Arrays;

import com.ebay.kvstore.util.IKeyComparable;
import com.ebay.kvstore.util.KeyValueUtil;

public class Region implements Serializable, Comparable<Region>, IKeyComparable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int regionId;
	private byte[] start;
	// null end is the max
	private byte[] end;
	private RegionStat stat;
	// whether the region has been changed after last stat.

	public Region(int regionId, byte[] start, byte[] end) {
		super();
		this.regionId = regionId;
		this.start = start;
		this.end = end;
		this.stat = new RegionStat();
	}

	@Override
	public int compareTo(byte[] key) {
		int e1 = KeyValueUtil.compare(key, start);
		int e2 = KeyValueUtil.compare(key, end);
		if (e1 >= 0 && e2 <= 0) {
			return 0;
		} else if (e1 < 0) {
			return 1;
		} else {
			return -1;
		}
	}

	@Override
	public int compareTo(Region r) {
		if (this == r) {
			return 0;
		}
		int e1 = KeyValueUtil.compare(start, r.getEnd());
		int e2 = KeyValueUtil.compare(end, r.getStart());
		if (e1 > 0) {
			return 1;
		} else if (e2 < 0) {
			return -1;
		} else {
			return 0;
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Region other = (Region) obj;
		if (regionId != other.regionId)
			return false;
		return true;
	}

	public byte[] getEnd() {
		return end;
	}

	public int getRegionId() {
		return regionId;
	}

	public byte[] getStart() {
		return start;
	}

	public RegionStat getStat() {
		return stat;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + regionId;
		return result;
	}

	public void setEnd(byte[] end) {
		this.end = end;
	}

	public void setRegionId(int regionId) {
		this.regionId = regionId;
	}

	public void setStart(byte[] start) {
		this.start = start;
	}

	public void setStat(RegionStat stat) {
		this.stat = stat;
	}

	@Override
	public String toString() {
		return "Region [regionId=" + regionId + ", start=" + Arrays.toString(start) + ", end="
				+ Arrays.toString(end) + "]";
	}
}
