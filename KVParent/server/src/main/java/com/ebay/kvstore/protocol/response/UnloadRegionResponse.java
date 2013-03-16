package com.ebay.kvstore.protocol.response;

import com.ebay.kvstore.protocol.ProtocolType;

public class UnloadRegionResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected int regionId;

	@Override
	public int getType() {
		return ProtocolType.Unload_Region_Resp;
	}

	public UnloadRegionResponse(int retCode, int regionId) {
		super(retCode);
		this.regionId = regionId;
	}

}
