package com.ebay.kvstore.structure;

import java.io.Serializable;

public class RegionStat implements Serializable, Cloneable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public int keyNum;
	public long size;
	public long readCount;
	public long writeCount;
	public boolean dirty;

	public RegionStat() {
		this(0, 0, 0, 0);
	}

	public RegionStat(int keyNum, long size, long readCount, long writeCount) {
		super();
		this.keyNum = keyNum;
		this.size = size;
		this.readCount = readCount;
		this.writeCount = writeCount;
		this.dirty = true;
	}

	public void reset() {
		this.keyNum = 0;
		this.size = 0;
	}

	@Override
	public String toString() {
		return "RegionStat [keyNum=" + keyNum + ", size=" + size + ", readCount=" + readCount
				+ ", writeCount=" + writeCount + "]";
	}

	public RegionStat clone() {
		try {
			return (RegionStat) super.clone();
		} catch (CloneNotSupportedException e) {
			return null;
		}
	}

}
