package com.ebay.kvstore.client.sync;

import com.ebay.kvstore.client.BaseClient;
import com.ebay.kvstore.client.ClientOption;
import com.ebay.kvstore.client.IClientHandler;
import com.ebay.kvstore.protocol.IProtocolType;

public class SyncKVClient extends BaseClient {

	public SyncKVClient(ClientOption option) {
		super(option);
		this.dispatcher.registerHandler(IProtocolType.Get_Resp, new SyncGetResponseHandler());
		this.dispatcher.registerHandler(IProtocolType.Set_Resp, new SyncSetResponseHandler());
		this.dispatcher.registerHandler(IProtocolType.Delete_Resp, new SyncDeleteResponseHandler());
		this.dispatcher.registerHandler(IProtocolType.Incr_Resp, new SyncIncrResponseHandler());
		this.dispatcher.registerHandler(IProtocolType.Stat_Resp, new SyncStatResponseHandler());

	}

	public boolean delete(byte[] key) {
		// TODO Auto-generated method stub
		return false;
	}

	public byte[] get(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	public IClientHandler getClientHandler() {
		throw new UnsupportedOperationException("Should not get IClientHandler for SyncKVClient");
	}

	public int getCounter(byte[] key) {
		// TODO Auto-generated method stub
		return 0;
	}

	public void incr(byte[] key, int intial, int incremental) {
		// TODO Auto-generated method stub

	}

	public void setHandler(IClientHandler handler) {
		throw new UnsupportedOperationException("Should not set IClientHandler for SyncKVClient");
	}

	public void stat() {
		// TODO Auto-generated method stub

	}

}
