package com.ebay.kvstore.server.data.storage.helper;

public interface IRegionFlushListener extends IRegionListener {
	public void onFlushBegin();

	public void onFlushCommit(boolean success, String path);

	public void onFlushEnd(boolean success, String path);
}
