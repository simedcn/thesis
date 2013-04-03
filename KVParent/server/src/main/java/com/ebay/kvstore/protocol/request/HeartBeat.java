package com.ebay.kvstore.protocol.request;

import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.structure.DataServerStruct;

public class HeartBeat extends ServerRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private DataServerStruct struct;

	public HeartBeat(DataServerStruct struct) {
		super();
		this.struct = struct;
	}

	public DataServerStruct getStruct() {
		return struct;
	}

	@Override
	public int getType() {
		return IProtocolType.Heart_Beart_Req;
	}

	public void setStruct(DataServerStruct struct) {
		this.struct = struct;
	}

	@Override
	public String toString() {
		return "HeartBeat [struct=" + struct + "]";
	}

}
