package com.ebay.kvstore.server.data.storage.task;

public interface IRegionFlushListener extends IRegionTaskListener {
	
	public void onFlushBegin();

	public void onFlushCommit(boolean success, String path);

	public void onFlushEnd(boolean success, String path);
}
