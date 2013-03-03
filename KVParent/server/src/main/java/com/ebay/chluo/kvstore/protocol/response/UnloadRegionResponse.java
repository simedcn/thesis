package com.ebay.chluo.kvstore.protocol.response;

import com.ebay.chluo.kvstore.protocol.ProtocolType;

public class UnloadRegionResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected int regionId;

	@Override
	public int getType() {
		return ProtocolType.Unload_Region;
	}

	public UnloadRegionResponse(int retCode, int regionId) {
		super(retCode);
		this.regionId = regionId;
	}

}
