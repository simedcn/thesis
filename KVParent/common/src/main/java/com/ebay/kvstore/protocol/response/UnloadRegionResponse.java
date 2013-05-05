package com.ebay.kvstore.protocol.response;

import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.structure.Region;

public class UnloadRegionResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected int regionId;

	protected Region region;

	public UnloadRegionResponse(int retCode, int regionId, Region region) {
		super(retCode);
		this.regionId = regionId;
		this.region = region;
	}

	public Region getRegion() {
		return region;
	}

	public int getRegionId() {
		return regionId;
	}

	@Override
	public int getType() {
		return IProtocolType.Unload_Region_Resp;
	}

	public void setRegion(Region region) {
		this.region = region;
	}

	public void setRegionId(int regionId) {
		this.regionId = regionId;
	}

	@Override
	public String toString() {
		return "UnloadRegionResponse [regionId=" + regionId + ", region=" + region + "]";
	}

}
