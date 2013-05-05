package com.ebay.kvstore.client.async;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.client.BaseKVClient;
import com.ebay.kvstore.client.ClientOption;
import com.ebay.kvstore.client.IKVClientHandler;
import com.ebay.kvstore.client.result.GetResult;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.IProtocolType;
import com.ebay.kvstore.protocol.request.DeleteRequest;
import com.ebay.kvstore.protocol.request.GetRequest;
import com.ebay.kvstore.protocol.request.IncrRequest;
import com.ebay.kvstore.protocol.request.RegionTableRequest;
import com.ebay.kvstore.protocol.request.SetRequest;
import com.ebay.kvstore.protocol.request.StatRequest;
import com.ebay.kvstore.structure.DataServerStruct;

public class AsyncKVClient extends BaseKVClient {

	private static Logger logger = LoggerFactory.getLogger(AsyncKVClient.class);

	protected IKVClientHandler handler;

	public AsyncKVClient(ClientOption option) {
		super(option);
		this.dispatcher.registerHandler(IProtocolType.Get_Resp, new AsyncGetResponseHandler());
		this.dispatcher.registerHandler(IProtocolType.Set_Resp, new AsyncSetResponseHandler());
		this.dispatcher
				.registerHandler(IProtocolType.Delete_Resp, new AsyncDeleteResponseHandler());
		this.dispatcher.registerHandler(IProtocolType.Incr_Resp, new AsyncIncrResponseHandler());
		this.dispatcher.registerHandler(IProtocolType.Stat_Resp, new AsyncStatResponseHandler());
		this.dispatcher.registerHandler(IProtocolType.Region_Table_Resp,
				new AsyncRegionTableResponseHandler());

	}

	@Override
	public void delete(byte[] key) throws KVException {
		checkKey(key);
		IoSession session = getConnection(key);
		IProtocol request = new DeleteRequest(key);
		session.write(request);
	}

	@Override
	public GetResult get(byte[] key) throws KVException {
		checkKey(key);
		IoSession session = getConnection(key);
		IProtocol request = new GetRequest(key);
		session.write(request);
		return null;
	}

	@Override
	public IKVClientHandler getClientHandler() {
		return handler;
	}

	@Override
	public GetResult getCounter(byte[] key) throws KVException {
		checkKey(key);
		IoSession session = getConnection(key);
		IProtocol request = new GetRequest(key);
		session.write(request);
		return null;
	}

	@Override
	public int incr(byte[] key, int incremental, int initValue) throws KVException {
		return incr(key, incremental, initValue, 0);
	}

	@Override
	public int incr(byte[] key, int incremental, int initValue, int ttl) throws KVException {
		checkKey(key);
		IoSession session = getConnection(key);
		IProtocol request = new IncrRequest(key, incremental, initValue, ttl);
		session.write(request);
		return 0;
	}

	@Override
	public void set(byte[] key, byte[] value) throws KVException {
		set(key, value, 0);
	}

	@Override
	public void set(byte[] key, byte[] value, int ttl) throws KVException {
		checkKey(key);
		if (value == null) {
			throw new NullPointerException("null value is not allowed");
		}
		IoSession session = getConnection(key);
		IProtocol request = new SetRequest(key, value, ttl);
		session.write(request);
	}

	@Override
	public void setHandler(IKVClientHandler handler) {
		this.handler = handler;
	}

	@Override
	public DataServerStruct[] stat() throws KVException {
		IoSession session = getMasterConnection();
		IProtocol request = new StatRequest(true);
		session.write(request);
		return null;
	}

	@Override
	public void updateRegionTable() throws KVException {
		IoSession session = getMasterConnection();
		IProtocol request = new RegionTableRequest();
		session.write(request);
		synchronized (this) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				logger.error("Update Region Table Operation has been interrupted", e);
			}
		}
	}
}
