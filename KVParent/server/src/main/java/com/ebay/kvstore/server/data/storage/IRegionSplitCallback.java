package com.ebay.kvstore.server.data.storage;

import com.ebay.kvstore.structure.Region;

public interface IRegionSplitCallback {
	public void callback(boolean success, Region oldRegion, Region newRegion);
}
