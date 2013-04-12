package com.ebay.kvstore.server.data.storage.task;

import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.structure.Region;

public interface IRegionMergeListener {

	public void onMergeBegin();

	public void onMergeCommit(boolean success, IRegionStorage storage);

	public Region onMergeEnd(boolean success, Region region1, Region region2);
}
