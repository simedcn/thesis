package com.ebay.chluo.kvstore.data.storage.fs;

import java.io.DataOutput;
import java.io.IOException;

public interface BlockOutputStream extends DataOutput {
	public int getCurrentBlock();

	public int getPos();

	public int getBlockPos();

	public int getBlockAvailable();

	public void close() throws IOException;
}
