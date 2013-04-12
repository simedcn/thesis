package com.ebay.kvstore.server.data.storage.task;

import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.structure.Region;

public interface IRegionSplitListener extends IRegionTaskListener {

	public void onSplitBegin();

	public void onSplitCommit(boolean success, IRegionStorage oldStorage, IRegionStorage newStorage);

	public Region onSplitEnd(boolean success, byte[] start, byte[] end);
}
