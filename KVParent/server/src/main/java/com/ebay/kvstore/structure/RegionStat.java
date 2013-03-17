package com.ebay.kvstore.structure;

import java.io.Serializable;

public class RegionStat implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public int keyNum;
	public long size;
	public int readCount;
	public int writeCount;
	public boolean dirty;

	public RegionStat() {
		this(0, 0, 0, 0);
	}

	public RegionStat(int keyNum, int size, int readCount, int writeCount) {
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

}
