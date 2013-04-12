package com.ebay.kvstore.server.data.storage;

import com.ebay.kvstore.structure.Region;

public interface IRegionMergeCallback {
	public void callback(boolean success, int region1, int region2, Region region);
}
