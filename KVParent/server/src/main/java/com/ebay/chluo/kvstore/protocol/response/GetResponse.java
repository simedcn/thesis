package com.ebay.chluo.kvstore.protocol.response;

import java.util.Arrays;

import com.ebay.chluo.kvstore.protocol.ProtocolType;

public class GetResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected byte[] key;
	protected byte[] value;
	protected int version;
	protected int createTime;
	protected int lastUpdateTime;

	@Override
	public int getType() {
		return ProtocolType.Get;
	}


	public GetResponse(int retCode, byte[] key, byte[] value, int version, int createTime,
			int lastUpdateTime) {
		super(retCode);
		this.key = key;
		this.value = value;
		this.version = version;
		this.createTime = createTime;
		this.lastUpdateTime = lastUpdateTime;
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

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
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

	@Override
	public String toString() {
		return "SetResponse [key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value)
				+ ", version=" + version + ", createTime=" + createTime + ", lastUpdateTime="
				+ lastUpdateTime + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + createTime;
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
		GetResponse other = (GetResponse) obj;
		if (createTime != other.createTime)
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
