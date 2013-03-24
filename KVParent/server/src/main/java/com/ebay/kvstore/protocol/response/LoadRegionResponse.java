package com.ebay.kvstore.protocol.response;

import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.structure.Region;

public class LoadRegionResponse extends BaseResponse {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected Region region;

	public LoadRegionResponse(int retCode, Region region) {
		super(retCode);
		this.region = region;
	}

	public Region getRegion() {
		return region;
	}

	@Override
	public int getType() {
		return IProtocolType.Load_Region_Resp;
	}

	public void setRegion(Region region) {
		this.region = region;
	}
}
