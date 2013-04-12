package com.ebay.kvstore.protocol.response;

import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.structure.Region;

public class MergeRegionResponse extends BaseResponse {

	private int regionId1;
	private int regionId2;
	private Region region;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int getType() {
		return IProtocolType.Merge_Region_Resp;
	}

	public MergeRegionResponse(int retCode, int regionId1, int regionId2, Region region) {
		super(retCode);
		this.regionId1 = regionId1;
		this.regionId2 = regionId2;
		this.region = region;
	}

	public int getRegionId1() {
		return regionId1;
	}

	public void setRegionId1(int regionId1) {
		this.regionId1 = regionId1;
	}

	public int getRegionId2() {
		return regionId2;
	}

	public void setRegionId2(int regionId2) {
		this.regionId2 = regionId2;
	}

	public Region getRegion() {
		return region;
	}

	public void setRegion(Region region) {
		this.region = region;
	}

	@Override
	public String toString() {
		return "MergeRegionResponse [regionId1=" + regionId1 + ", regionId2=" + regionId2
				+ ", region=" + region + "]";
	}

}
