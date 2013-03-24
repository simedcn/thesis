package com.ebay.kvstore.protocol.request;

import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.structure.Region;

public class LoadRegionRequest extends ServerRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Region region;

	public LoadRegionRequest(Region region) {
		super();
		this.region = region;
	}

	public Region getRegion() {
		return region;
	}

	@Override
	public int getType() {
		return IProtocolType.Load_Region_Req;
	}

	public void setRegion(Region region) {
		this.region = region;
	}

	@Override
	public String toString() {
		return "LoadRegionRequest [region=" + region + "]";
	}

}
