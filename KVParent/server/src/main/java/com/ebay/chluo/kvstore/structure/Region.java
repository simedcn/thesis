package com.ebay.chluo.kvstore.structure;

import java.io.Serializable;
import java.util.Arrays;

public class Region implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int regionId;
	private byte[] start;
	private byte[] end;
	private RegionStat stat;

	public Region(int regionId, byte[] start, byte[] end, RegionStat stat) {
		super();
		this.regionId = regionId;
		this.start = start;
		this.end = end;
		this.stat = stat;
	}

	public int getRegionId() {
		return regionId;
	}

	public void setRegionId(int regionId) {
		this.regionId = regionId;
	}

	public byte[] getStart() {
		return start;
	}

	public void setStart(byte[] start) {
		this.start = start;
	}

	public byte[] getEnd() {
		return end;
	}

	public void setEnd(byte[] end) {
		this.end = end;
	}

	@Override
	public String toString() {
		return "Region [regionId=" + regionId + ", start=" + Arrays.toString(start) + ", end="
				+ Arrays.toString(end) + "]";
	}

	public RegionStat getStat() {
		return stat;
	}

	public void setStat(RegionStat stat) {
		this.stat = stat;
	}

}
