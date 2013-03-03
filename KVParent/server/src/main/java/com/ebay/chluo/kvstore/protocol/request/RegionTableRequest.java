package com.ebay.chluo.kvstore.protocol.request;

import com.ebay.chluo.kvstore.protocol.ProtocolType;

public class RegionTableRequest extends ClientRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int getType() {
		return ProtocolType.Region_Table_Req;
	}

}
