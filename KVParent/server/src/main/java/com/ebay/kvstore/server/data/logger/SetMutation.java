package com.ebay.kvstore.server.data.logger;

import java.io.IOException;
import java.util.Arrays;

import com.ebay.kvstore.logger.ILoggerInputStream;
import com.ebay.kvstore.logger.ILoggerOutputStream;
import com.ebay.kvstore.structure.Value;

public class SetMutation implements IMutation {
	protected byte[] key;
	
	protected Value value;

	public SetMutation(byte[] key, Value value) {
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
	public Value getValue() {
		return value;
	}

	@Override
	public void readFromExternal(ILoggerInputStream in) throws IOException {
		int keyLen = in.readInt();
		key = new byte[keyLen];
		in.read(key);
		int valueLen = in.readInt();
		byte[] bytes = new byte[valueLen];
		in.read(bytes);
		long expire = in.readLong();
		value = new Value(bytes, expire);
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public void setValue(Value value) {
		this.value = value;
	}

	@Override
	public void writeToExternal(ILoggerOutputStream out) throws IOException {
		out.write(getType());
		out.writeInt(key.length);
		out.write(key);
		out.writeInt(value.getValue().length);
		out.write(value.getValue());
		out.writeLong(value.getExpire());
	}

	@Override
	public String toString() {
		return "SetMutation [key=" + Arrays.toString(key) + ", value=" + value + "]";
	}

}
