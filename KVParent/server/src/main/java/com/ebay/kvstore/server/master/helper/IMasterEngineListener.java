package com.ebay.kvstore.server.master.helper;

import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public interface IMasterEngineListener {

	public void onDataServerUnload(DataServerStruct struct);

	public void onRegionLoad(DataServerStruct struct, Region region);

	public void onRegionSplit(Region oldRegion, Region newRegion);

	public void onRegionUnload(DataServerStruct struct, Region region);
}
