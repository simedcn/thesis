package com.ebay.kvstore.client.result;

import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.structure.DataServerStruct;

public class StatResult extends BaseResult {

	private DataServerStruct[] dataServers;

	public StatResult(DataServerStruct[] dataServers, KVException e) {
		super(null, e);
		this.dataServers = dataServers;
	}

	public DataServerStruct[] getDataServers() throws KVException {
		checkException();
		return dataServers;
	}

}
