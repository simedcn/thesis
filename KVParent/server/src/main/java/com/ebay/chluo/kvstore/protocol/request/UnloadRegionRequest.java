package com.ebay.chluo.kvstore.protocol.request;

import com.ebay.chluo.kvstore.protocol.ProtocolType;

public class UnloadRegionRequest extends ServerRequest {

	public static final byte StartUnload = 0;

	public static final byte CommitUnload = 1;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected int regionId;

	protected byte phase;

	@Override
	public int getType() {
		return ProtocolType.Unload_Region;
	}

	public UnloadRegionRequest(int regionId, byte phase) {
		super();
		this.regionId = regionId;
		this.phase = phase;
	}

	public int getRegionId() {
		return regionId;
	}

	public byte getPhase() {
		return phase;
	}

	public void setPhase(byte phase) {
		this.phase = phase;
	}

	public void setRegionId(int regionId) {
		this.regionId = regionId;
	}

	@Override
	public String toString() {
		return "UnloadRegionRequest [regionId=" + regionId + ", phase=" + phase + "]";
	}

}
