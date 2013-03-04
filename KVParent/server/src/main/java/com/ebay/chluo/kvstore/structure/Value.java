package com.ebay.chluo.kvstore.structure;

import java.io.Serializable;

public class Value implements Serializable {
	private byte[] value;
	private int createTime;
	private int lastUpdateTime;
	private int version;
	private boolean deleted;

	public Value(byte[] value, int createTime, int lastUpdateTime, int version, boolean deleted) {
		super();
		this.value = value;
		this.createTime = createTime;
		this.lastUpdateTime = lastUpdateTime;
		this.version = version;
		this.deleted = deleted;
	}

}
