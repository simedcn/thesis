package com.ebay.kvstore.server.master.helper;

import java.util.List;

import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

/**
 * 
 * @author luochen
 * 
 */
public interface ILoadBalancer {

	public LBResult balance(List<DataServerStruct> list);

}

class LBResult {
	public DataServerStruct source;
	public DataServerStruct target;
	public Region region;
}
