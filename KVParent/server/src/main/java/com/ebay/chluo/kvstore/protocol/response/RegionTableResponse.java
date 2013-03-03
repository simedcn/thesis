package com.ebay.chluo.kvstore.protocol.response;

import java.util.Arrays;

import com.ebay.chluo.kvstore.protocol.ProtocolType;
import com.ebay.chluo.kvstore.structure.Region;

public class RegionTableResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected Region[] regions;

	@Override
	public int getType() {
		return ProtocolType.Region_Table_Resp;
	}

	public RegionTableResponse(int retCode, Region[] regions) {
		super(retCode);
		this.regions = regions;
	}

	public Region[] getRegions() {
		return regions;
	}

	public void setRegions(Region[] regions) {
		this.regions = regions;
	}

	@Override
	public String toString() {
		return "RegionTableResponse [regions=" + Arrays.toString(regions) + "]";
	}

}
