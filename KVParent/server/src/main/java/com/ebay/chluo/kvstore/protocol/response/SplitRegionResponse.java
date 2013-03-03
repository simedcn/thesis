package com.ebay.chluo.kvstore.protocol.response;

import com.ebay.chluo.kvstore.protocol.ProtocolType;
import com.ebay.chluo.kvstore.structure.Region;

public class SplitRegionResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected Region orgRegion;

	protected Region newRegion;

	public SplitRegionResponse(int retCode, Region orgRegion, Region newRegion) {
		super(retCode);
		this.orgRegion = orgRegion;
		this.newRegion = newRegion;
	}

	@Override
	public int getType() {
		return ProtocolType.Split_Region;
	}


	public Region getOrgRegion() {
		return orgRegion;
	}

	public void setOrgRegion(Region orgRegion) {
		this.orgRegion = orgRegion;
	}

	public Region getNewRegion() {
		return newRegion;
	}

	public void setNewRegion(Region newRegion) {
		this.newRegion = newRegion;
	}

	@Override
	public String toString() {
		return "SplitRegionResponse [orgRegion=" + orgRegion + ", newRegion=" + newRegion + "]";
	}

}
