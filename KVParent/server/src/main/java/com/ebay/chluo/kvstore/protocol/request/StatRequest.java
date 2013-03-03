package com.ebay.chluo.kvstore.protocol.request;

import com.ebay.chluo.kvstore.protocol.ProtocolType;

public class StatRequest extends ClientRequest{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int getType() {
		return ProtocolType.Stat;
	}

}
