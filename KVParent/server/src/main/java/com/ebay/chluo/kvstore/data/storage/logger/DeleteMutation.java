package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class DeleteMutation implements IMutation {
	protected byte[] key;

	@Override
	public int getType() {
		return Delete;
	}

	@Override
	public void writeToExternal(OutputStream out) {
		// TODO Auto-generated method stub

	}

	@Override
	public void readFromExternal(InputStream in) {
		// TODO Auto-generated method stub

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

}
