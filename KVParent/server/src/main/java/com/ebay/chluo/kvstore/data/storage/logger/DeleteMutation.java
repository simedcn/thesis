package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.IOException;
import java.util.Arrays;

public class DeleteMutation implements IMutation {
	protected byte[] key;

	@Override
	public byte getType() {
		return Delete;
	}

	@Override
	public void writeToExternal(LoggerOutputStream out) throws IOException {
		out.write(getType());
		out.writeInt(key.length);
		out.write(key);
	}

	@Override
	public void readFromExternal(LoggerInputStream in) throws IOException {
		int length = in.readInt();
		key = new byte[length];
		in.read(key);
	}

	public DeleteMutation(byte[] key) {
		super();
		this.key = key;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	@Override
	public String toString() {
		return "DeleteMutation [key=" + Arrays.toString(key) + "]";
	}

	@Override
	public byte[] getValue() {
		return null;
	}

}
