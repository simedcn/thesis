package com.ebay.kvstore.server.data.storage.task;

import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;

public interface IRegionLoadListener extends IRegionTaskListener {

	public void onLoadBegin();

	public void onLoadCommit(boolean success, IRegionStorage storage);

	public void onLoadEnd(boolean success);

}
