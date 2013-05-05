package com.ebay.kvstore.protocol.request;

import com.ebay.kvstore.protocol.IProtocolType;

public class StatRequest extends ClientRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public StatRequest(boolean retry) {
		super(retry);
	}

	@Override
	public int getType() {
		return IProtocolType.Stat_Req;
	}

	@Override
	public String toString() {
		return "StatRequest []";
	}

}
