package com.ebay.kvstore.server.data.storage;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.Value;

public class StoreLogListener implements IStoreListener {

	private static Logger logger = LoggerFactory.getLogger(StoreLogListener.class);

	@Override
	public void onSet(Region region, byte[] key, byte[] value) {
		logger.info("set key:" + Arrays.toString(key) + " value:" + Arrays.toString(value));
	}

	@Override
	public void onGet(Region region, byte[] key, Value value) {
		logger.info("get key:" + Arrays.toString(key) + " value:" + value.toString());
	}

	@Override
	public void onDelete(Region region, byte[] key) {
		logger.info("delete key:" + Arrays.toString(key));
	}

	@Override
	public void onIncr(Region region, byte[] key, int value) {
		logger.info("incr key:" + Arrays.toString(key) + " value:" + value);
	}

	@Override
	public void onSplit(Region oldRegion, Region newRegion) {
		logger.info("Region " + oldRegion.getRegionId() + " split into two new regions:"
				+ oldRegion.toString() + "\t" + newRegion + toString());
	}

	@Override
	public void onLoad(Region region) {
		logger.info("New region loaded:" + region.toString());
	}

}
