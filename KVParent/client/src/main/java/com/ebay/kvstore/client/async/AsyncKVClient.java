package com.ebay.kvstore.client.async;

import com.ebay.kvstore.client.BaseClient;
import com.ebay.kvstore.client.ClientOption;
import com.ebay.kvstore.client.IClientHandler;
import com.ebay.kvstore.protocol.IProtocolType;

public class AsyncKVClient extends BaseClient {

	protected IClientHandler handler;

	public AsyncKVClient(ClientOption option) {
		super(option);
		this.dispatcher.registerHandler(IProtocolType.Get_Resp, new AsyncGetResponseHandler());
		this.dispatcher.registerHandler(IProtocolType.Set_Resp, new AsyncSetResponseHandler());
		this.dispatcher
				.registerHandler(IProtocolType.Delete_Resp, new AsyncDeleteResponseHandler());
		this.dispatcher.registerHandler(IProtocolType.Incr_Resp, new AsyncIncrResponseHandler());
		this.dispatcher.registerHandler(IProtocolType.Stat_Resp, new AsyncStatResponseHandler());

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
		return handler;
	}

	public int getCounter(byte[] key) {
		// TODO Auto-generated method stub
		return 0;
	}

	public void incr(byte[] key, int intial, int incremental) {
		// TODO Auto-generated method stub

	}

	public void setHandler(IClientHandler handler) {
		this.handler = handler;
	}

	public void stat() {
		// TODO Auto-generated method stub

	}

}
