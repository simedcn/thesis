package com.ebay.kvstore.structure;

import java.io.Serializable;

public class RegionStat implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int keyNum;
	private int size;
	private int readCount;
	private int writeCount;

	public RegionStat() {
		this(0, 0, 0, 0);
	}

	public RegionStat(int keyNum, int size, int readCount, int writeCount) {
		super();
		this.keyNum = keyNum;
		this.size = size;
		this.readCount = readCount;
		this.writeCount = writeCount;
	}

	public int getKeyNum() {
		return keyNum;
	}

	public void setKeyNum(int keyNum) {
		this.keyNum = keyNum;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getReadCount() {
		return readCount;
	}

	public void setReadCount(int readCount) {
		this.readCount = readCount;
	}

	public int getWriteCount() {
		return writeCount;
	}

	public void setWriteCount(int writeCount) {
		this.writeCount = writeCount;
	}

	@Override
	public String toString() {
		return "RegionStat [keyNum=" + keyNum + ", size=" + size + ", readCount=" + readCount
				+ ", writeCount=" + writeCount + "]";
	}
}
