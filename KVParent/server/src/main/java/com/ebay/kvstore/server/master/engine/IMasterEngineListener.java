package com.ebay.kvstore.server.master.engine;

import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public interface IMasterEngineListener {

	public void onDataServerUnload(DataServerStruct struct);

	public void onRegionLoad(DataServerStruct struct, Region region);

	public void onRegionSplit(int oldRegion, int newRegion);

	public void onRegionUnload(DataServerStruct struct, int region);

	public void onRegionMerge(DataServerStruct struct, int regionId1, int regionId2);
}
