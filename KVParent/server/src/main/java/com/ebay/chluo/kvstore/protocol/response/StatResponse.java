package com.ebay.chluo.kvstore.protocol.response;

import java.util.Arrays;

import com.ebay.chluo.kvstore.protocol.ProtocolType;
import com.ebay.chluo.kvstore.structure.DataServerStruct;

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
