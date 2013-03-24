package com.ebay.kvstore.server.data.logger;

import java.io.IOException;
import java.util.Arrays;

import com.ebay.kvstore.logger.ILoggerInputStream;
import com.ebay.kvstore.logger.ILoggerOutputStream;

public class SetMutation implements IMutation {
	protected byte[] key;
	protected byte[] value;

	public SetMutation(byte[] key, byte[] value) {
		super();
		this.key = key;
		this.value = value;
	}

	@Override
	public byte[] getKey() {
		return key;
	}

	@Override
	public byte getType() {
		return Set;
	}

	@Override
	public byte[] getValue() {
		return value;
	}

	@Override
	public void readFromExternal(ILoggerInputStream in) throws IOException {
		int keyLen = in.readInt();
		key = new byte[keyLen];
		in.read(key);
		int valueLen = in.readInt();
		value = new byte[valueLen];
		in.read(value);
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "SetMutation [key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value)
				+ "]";
	}

	@Override
	public void writeToExternal(ILoggerOutputStream out) throws IOException {
		out.write(getType());
		out.writeInt(key.length);
		out.write(key);
		out.writeInt(value.length);
		out.write(value);
	}

}
