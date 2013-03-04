package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class SetMutation implements IMutation {
	protected byte[] key;
	protected byte[] value;

	@Override
	public int getType() {
		return Set;
	}

	@Override
	public void writeToExternal(OutputStream out) {

	}

	@Override
	public void readFromExternal(InputStream in) {

	}

	public SetMutation(byte[] key, byte[] value) {
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

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "SetMutation [key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value)
				+ "]";
	}

}
