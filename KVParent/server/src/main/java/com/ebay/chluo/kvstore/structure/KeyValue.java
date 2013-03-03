package com.ebay.chluo.kvstore.structure;

import java.io.Serializable;
import java.util.Arrays;

public class KeyValue implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private byte[] key;
	private byte[] value;
	private int createTime;
	private int lastUpdateTime;
	private int version;
	private boolean deleted;

	public KeyValue(byte[] key, byte[] value, int createTime, int lastUpdateTime, int version,
			boolean deleted) {
		super();
		this.key = key;
		this.value = value;
		this.createTime = createTime;
		this.lastUpdateTime = lastUpdateTime;
		this.version = version;
		this.deleted = deleted;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public int getCreateTime() {
		return createTime;
	}

	public void setCreateTime(int createTime) {
		this.createTime = createTime;
	}

	public int getLastUpdateTime() {
		return lastUpdateTime;
	}

	public void setLastUpdateTime(int lastUpdateTime) {
		this.lastUpdateTime = lastUpdateTime;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public boolean isDeleted() {
		return deleted;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	@Override
	public String toString() {
		return "KeyValue [key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value)
				+ ", createTime=" + createTime + ", lastUpdateTime=" + lastUpdateTime
				+ ", version=" + version + ", deleted=" + deleted + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + createTime;
		result = prime * result + (deleted ? 1231 : 1237);
		result = prime * result + Arrays.hashCode(key);
		result = prime * result + lastUpdateTime;
		result = prime * result + Arrays.hashCode(value);
		result = prime * result + version;
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
		KeyValue other = (KeyValue) obj;
		if (createTime != other.createTime)
			return false;
		if (deleted != other.deleted)
			return false;
		if (!Arrays.equals(key, other.key))
			return false;
		if (lastUpdateTime != other.lastUpdateTime)
			return false;
		if (!Arrays.equals(value, other.value))
			return false;
		if (version != other.version)
			return false;
		return true;
	}

}
