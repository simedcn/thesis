package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.IOException;
import java.util.Arrays;

public class SetMutation implements IMutation {
	protected byte[] key;
	protected byte[] value;

	@Override
	public byte getType() {
		return Set;
	}

	public SetMutation(byte[] key, byte[] value) {
		super();
		this.key = key;
		this.value = value;
	}

	@Override
	public void readFromExternal(LoggerInputStream in) throws IOException {
		int keyLen = in.readInt();
		key = new byte[keyLen];
		in.read(key);
		int valueLen = in.readInt();
		value = new byte[valueLen];
		in.read(value);
	}

	@Override
	public void writeToExternal(LoggerOutputStream out) throws IOException {
		out.write(getType());
		out.writeInt(key.length);
		out.write(key);
		out.writeInt(value.length);
		out.write(value);
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
