package com.ebay.chluo.kvstore.structure;

import java.io.Serializable;
import java.util.Arrays;

import com.ebay.chluo.kvstore.KeyValueUtil;

public class Value implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private byte[] value;
	private boolean deleted;

	public int getSize() {
		return 9 + value.length;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public boolean isDeleted() {
		return deleted;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	public void incr(int incremental) {
		// the default value is 0
		if (value.length != 4) {
			throw new UnsupportedOperationException("The current value:" + value
					+ " is not a valid integer");
		}
		int counter = KeyValueUtil.byte2int(value);
		counter += incremental;
		value = KeyValueUtil.intToBytes(counter);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(value);
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
		Value other = (Value) obj;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}

	public Value(byte[] value) {
		super();
		this.value = value;
		this.deleted = false;
	}

	public Value(byte[] value, boolean deleted) {
		this.value = value;
		this.deleted = deleted;
	}

}
