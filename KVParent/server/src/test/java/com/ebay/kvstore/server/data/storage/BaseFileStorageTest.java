package com.ebay.kvstore.server.data.storage;

import com.ebay.kvstore.server.data.storage.fs.IRegionStorage;
import com.ebay.kvstore.structure.Region;

public abstract class BaseFileStorageTest extends BaseFileTest{
	protected IRegionStorage storage;
	
	protected Region region;
}
