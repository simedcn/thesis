package com.ebay.kvstore.protocol.response;

import java.util.Arrays;

import com.ebay.kvstore.protocol.ProtocolType;
import com.ebay.kvstore.structure.DataServerStruct;

public class StatResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected DataServerStruct[] servers;

	@Override
	public int getType() {
		return ProtocolType.Stat_Resp;
	}

	public StatResponse(int retCode, DataServerStruct[] servers) {
		super(retCode);
		this.servers = servers;
	}

	@Override
	public String toString() {
		return "StatResponse [servers=" + Arrays.toString(servers) + "]";
	}

	public DataServerStruct[] getServers() {
		return servers;
	}

	public void setServers(DataServerStruct[] servers) {
		this.servers = servers;
	}

}
