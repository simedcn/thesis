package com.ebay.kvstore.protocol.response;

import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.structure.Region;

public class SplitRegionResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected Region oldRegion;

	protected Region newRegion;

	public SplitRegionResponse(int retCode, Region oldRegion, Region newRegion) {
		super(retCode);
		this.oldRegion = oldRegion;
		this.newRegion = newRegion;
	}

	public Region getNewRegion() {
		return newRegion;
	}

	public Region getOldRegion() {
		return oldRegion;
	}

	@Override
	public int getType() {
		return IProtocolType.Split_Region_Resp;
	}

	public void setNewRegion(Region newRegion) {
		this.newRegion = newRegion;
	}

	public void setOldRegion(Region oldRegion) {
		this.oldRegion = oldRegion;
	}

	@Override
	public String toString() {
		return "SplitRegionResponse [oldegion=" + oldRegion + ", newRegion=" + newRegion + "]";
	}

}
