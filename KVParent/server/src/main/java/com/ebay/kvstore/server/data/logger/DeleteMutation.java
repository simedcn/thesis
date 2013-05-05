package com.ebay.kvstore.server.data.logger;

import java.io.IOException;
import java.util.Arrays;

import com.ebay.kvstore.server.logger.ILoggerInputStream;
import com.ebay.kvstore.server.logger.ILoggerOutputStream;
import com.ebay.kvstore.structure.Value;

public class DeleteMutation implements IMutation {
	protected byte[] key;

	public DeleteMutation(byte[] key) {
		super();
		this.key = key;
	}

	@Override
	public byte[] getKey() {
		return key;
	}

	@Override
	public byte getType() {
		return Delete;
	}

	@Override
	public Value getValue() {
		return null;
	}

	@Override
	public void readFromExternal(ILoggerInputStream in) throws IOException {
		int length = in.readInt();
		key = new byte[length];
		in.read(key);
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	@Override
	public String toString() {
		return "DeleteMutation [key=" + Arrays.toString(key) + "]";
	}

	@Override
	public void writeToExternal(ILoggerOutputStream out) throws IOException {
		out.write(getType());
		out.writeInt(key.length);
		out.write(key);
	}

}
