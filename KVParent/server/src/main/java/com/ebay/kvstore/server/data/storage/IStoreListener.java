package com.ebay.kvstore.server.data.storage;

import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.Value;

public interface IStoreListener {
	public void onDelete(Region region, byte[] key);

	public void onGet(Region region, byte[] key, Value value);

	public void onIncr(Region region, byte[] key, int value);

	public void onLoad(Region region);

	public void onSet(Region region, byte[] key, byte[] value);

	public void onSplit(Region oldRegion, Region newRegion);

}
