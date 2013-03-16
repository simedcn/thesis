package com.ebay.kvstore.protocol.request;

import com.ebay.kvstore.protocol.ProtocolType;

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

	@Override
	public int getType() {
		return ProtocolType.Split_Region_Req;
	}

	public int getRegionId() {
		return regionId;
	}

	public void setRegionId(int regionId) {
		this.regionId = regionId;
	}

	public int getNewId() {
		return newId;
	}

	public void setNewId(int newId) {
		this.newId = newId;
	}

	@Override
	public String toString() {
		return "SplitRegionRequest [regionId=" + regionId + ", newId=" + newId + "]";
	}

}
