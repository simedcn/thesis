package com.ebay.kvstore.protocol.response;

import com.ebay.kvstore.protocol.IProtocolType;

public class DataServerJoinResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DataServerJoinResponse(int retCode) {
		super(retCode);
	}

	@Override
	public int getType() {
		return IProtocolType.DataServer_Join_Response;
	}

}
