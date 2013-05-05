package com.ebay.kvstore.protocol.request;

import com.ebay.kvstore.protocol.IProtocolType;

public class SplitRegionRequest extends ServerRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected int regionId;

	protected int newId;

	public SplitRegionRequest(int regionId, int newId) {
		super();
		this.regionId = regionId;
		this.newId = newId;
	}

	public int getNewId() {
		return newId;
	}

	public int getRegionId() {
		return regionId;
	}

	@Override
	public int getType() {
		return IProtocolType.Split_Region_Req;
	}

	public void setNewId(int newId) {
		this.newId = newId;
	}

	public void setRegionId(int regionId) {
		this.regionId = regionId;
	}

	@Override
	public String toString() {
		return "SplitRegionRequest [regionId=" + regionId + ", newId=" + newId + "]";
	}

}
