package com.ebay.chluo.kvstore.protocol.request;

import com.ebay.chluo.kvstore.protocol.ProtocolType;
import com.ebay.chluo.kvstore.structure.Region;

public class HeartBeatRequest extends ServerRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected Region[] regions;

	@Override
	public int getType() {
		return ProtocolType.Heart_Beart;
	}

	public HeartBeatRequest(Region[] regions) {
		super();
		this.regions = regions;
	}

	public Region[] getRegions() {
		return regions;
	}

	public void setRegions(Region[] regions) {
		this.regions = regions;
	}

}
