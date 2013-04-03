package com.ebay.kvstore.protocol.request;

import com.ebay.kvstore.protocol.IProtocolType;

public class RegionTableRequest extends ClientRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int getType() {
		return IProtocolType.Region_Table_Req;
	}

	@Override
	public String toString() {
		return "RegionTableRequest []";
	}

}
