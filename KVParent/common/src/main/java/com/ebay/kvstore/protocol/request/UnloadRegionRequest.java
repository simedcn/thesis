package com.ebay.kvstore.protocol.request;

import com.ebay.kvstore.protocol.IProtocolType;

public class UnloadRegionRequest extends ServerRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected int regionId;

	public UnloadRegionRequest(int regionId) {
		super();
		this.regionId = regionId;
	}

	public int getRegionId() {
		return regionId;
	}

	@Override
	public int getType() {
		return IProtocolType.Unload_Region_Req;
	}

	public void setRegionId(int regionId) {
		this.regionId = regionId;
	}

	@Override
	public String toString() {
		return "UnloadRegionRequest [regionId=" + regionId + "]";
	}

}
