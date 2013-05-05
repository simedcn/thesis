package com.ebay.kvstore.protocol.request;

import com.ebay.kvstore.protocol.IProtocolType;

public class MergeRegionRequest extends ServerRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private int regionId1;

	private int regionId2;

	private int newRegionId;

	public MergeRegionRequest(int regionId1, int regionId2, int newRegionId) {
		super();
		this.regionId1 = regionId1;
		this.regionId2 = regionId2;
		this.newRegionId = newRegionId;
	}

	public int getNewRegionId() {
		return newRegionId;
	}

	public int getRegionId1() {
		return regionId1;
	}

	public int getRegionId2() {
		return regionId2;
	}

	@Override
	public int getType() {
		return IProtocolType.Merge_Reigon_Req;
	}

	public void setNewRegionId(int newRegionId) {
		this.newRegionId = newRegionId;
	}
	
	public void setRegionId1(int regionId1) {
		this.regionId1 = regionId1;
	}
	
	public void setRegionId2(int regionId2) {
		this.regionId2 = regionId2;
	}

	@Override
	public String toString() {
		return "MergeRegionRequest [regionId1=" + regionId1 + ", regionId2=" + regionId2
				+ ", newRegionId=" + newRegionId + "]";
	}
}
