package com.ebay.kvstore.structure;

import java.io.Serializable;
import java.util.Arrays;

public class KeyValue implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected byte[] key;
	protected Value value;

	public KeyValue(byte[] key, Value value) {
		super();
		this.key = key;
		this.value = value;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public Value getValue() {
		return value;
	}

	public void setValue(Value value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(key);
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		if (!Arrays.equals(key, other.key))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "KeyValue [key=" + Arrays.toString(key) + ", value=" + value + "]";
	}

}
