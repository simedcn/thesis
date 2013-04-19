package com.ebay.kvstore.structure;

import java.io.Serializable;
import java.util.Arrays;

import com.ebay.kvstore.KeyValueUtil;

public class Value implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private byte[] value;
	private boolean deleted = false;
	private long expire = 0;;

	public Value(byte[] value) {
		super();
		this.value = value;
	}

	public Value(byte[] value, long expire) {
		this.value = value;
		this.expire = expire;
	}

	public Value(byte[] value, boolean deleted) {
		this.value = value;
		this.deleted = deleted;
	}
	public long getExpire() {
		return expire;
	}
	
	public void setExpire(long expire) {
		this.expire = expire;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Value other = (Value) obj;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}

	public byte[] getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(value);
		return result;
	}

	public void incr(int incremental) {
		// the default value is 0
		if (value.length != 4) {
			throw new UnsupportedOperationException("The current value:" + value
					+ " is not a valid integer");
		}
		int counter = KeyValueUtil.bytesToInt(value);
		counter += incremental;
		value = KeyValueUtil.intToBytes(counter);
	}

	public boolean isDeleted() {
		return deleted;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Value [value=" + Arrays.toString(value) + ", deleted=" + deleted + "]";
	}

}
