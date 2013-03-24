package com.ebay.kvstore.server.data.storage;

import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;
import com.ebay.kvstore.structure.Value;

public class StoreStatListener implements IStoreListener {
	private RegionStat stat;

	@Override
	public void onDelete(Region region, byte[] key) {
		stat = region.getStat();
		stat.writeCount++;
		stat.dirty = true;
	}

	@Override
	public void onGet(Region region, byte[] key, Value value) {
		stat = region.getStat();
		stat.readCount++;
	}

	@Override
	public void onIncr(Region region, byte[] key, int value) {
		stat = region.getStat();
		stat.writeCount++;
		stat.dirty = true;
	}

	@Override
	public void onLoad(Region region) {
		stat = region.getStat();
		stat.dirty = true;
	}

	@Override
	public void onSet(Region region, byte[] key, byte[] value) {
		stat = region.getStat();
		stat.writeCount++;
		stat.dirty = true;
	}

	@Override
	public void onSplit(Region oldRegion, Region newRegion) {
		stat = oldRegion.getStat();
		stat.dirty = true;
	}

}
