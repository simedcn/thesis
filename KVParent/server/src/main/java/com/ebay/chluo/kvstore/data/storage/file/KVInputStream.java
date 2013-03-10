package com.ebay.chluo.kvstore.data.storage.file;

import java.io.DataInput;
import java.io.IOException;

public interface KVInputStream extends DataInput {

	public int getCurrentBlock();

	public int getPos();

	public int getBlockPos();

	public int getBlockAvailable();

	public void close() throws IOException;
}
