package com.ebay.kvstore.server.data.storage.helper;

import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;

public interface IRegionLoadListener {

	public void onLoadBegin();

	public void onLoadCommit(boolean success, IRegionStorage storage);

	public void onLoadEnd(boolean success);

}
