package com.ebay.kvstore.server.data.storage.helper;

import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.structure.Region;

public interface IRegionSplitListener extends IRegionListener {

	public void onSplitBegin();

	public void onSplitCommit(boolean success, IRegionStorage oldStorage, IRegionStorage newStorage);

	/**
	 * Caller should return a valid regionId.
	 * 
	 * @param success
	 * @param bs
	 * @param newKeyStart
	 * @return
	 */
	public Region onSplitEnd(boolean success, byte[] start, byte[] end);
}
