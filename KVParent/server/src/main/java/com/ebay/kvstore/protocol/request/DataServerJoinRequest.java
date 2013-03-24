package com.ebay.kvstore.protocol.request;

import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.structure.DataServerStruct;

public class DataServerJoinRequest extends ServerRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private DataServerStruct struct;

	public DataServerJoinRequest(DataServerStruct struct) {
		super();
		this.struct = struct;
	}

	public DataServerStruct getStruct() {
		return struct;
	}

	@Override
	public int getType() {
		return IProtocolType.DataServer_Join_Request;
	}

	public void setStruct(DataServerStruct struct) {
		this.struct = struct;
	}

}
