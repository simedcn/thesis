package com.ebay.chluo.kvstore.protocol.request;

import com.ebay.chluo.kvstore.protocol.ProtocolType;
import com.ebay.chluo.kvstore.structure.Region;

public class LoadRegionRequest extends ServerRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Region region;

	private String srcIp;

	@Override
	public int getType() {
		return ProtocolType.Load_Region;
	}

	public LoadRegionRequest(Region region, String srcIp) {
		super();
		this.region = region;
		this.srcIp = srcIp;
	}

	public Region getRegion() {
		return region;
	}

	public void setRegion(Region region) {
		this.region = region;
	}

	public String getSrcIp() {
		return srcIp;
	}

	public void setSrcIp(String srcIp) {
		this.srcIp = srcIp;
	}

	@Override
	public String toString() {
		return "LoadRegionRequest [region=" + region + ", srcIp=" + srcIp + "]";
	}

}
