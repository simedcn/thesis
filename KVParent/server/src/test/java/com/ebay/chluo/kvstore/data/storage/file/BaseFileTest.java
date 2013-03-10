package com.ebay.chluo.kvstore.data.storage.file;

import java.io.File;

public class BaseFileTest {
	protected File file;

	protected String path = "test.data";

	protected KVOutputStream out;
	protected KVInputStream in;
	protected KeyValueInputIterator it;

	protected final int blockSize = 16;

}
