package com.ebay.chluo.kvstore.protocol.response;

import com.ebay.chluo.kvstore.protocol.ProtocolType;
import com.ebay.chluo.kvstore.structure.Region;

public class LoadRegionResponse extends BaseResponse {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected Region region;

	@Override
	public int getType() {
		return ProtocolType.Load_Region_Resp;
	}

	public Region getRegion() {
		return region;
	}

	public void setRegion(Region region) {
		this.region = region;
	}

	public LoadRegionResponse(int retCode, Region region) {
		super(retCode);
		this.region = region;
	}
}
