package com.ebay.chluo.kvstore.data.storage.file;

import java.io.File;

public interface FlushListener {
	public void onFlushBegin();

	public void onFlushEnd(boolean success, File file);

	public void onFlushCommit(File file);
}
