package com.ebay.kvstore.client.sync;

import org.apache.mina.core.future.ReadFuture;
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
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.context.IContext;
import com.ebay.kvstore.protocol.handler.IProtocolHandler;
import com.ebay.kvstore.protocol.request.DeleteRequest;
import com.ebay.kvstore.protocol.request.GetRequest;
import com.ebay.kvstore.protocol.request.IncrRequest;
import com.ebay.kvstore.protocol.request.RegionTableRequest;
import com.ebay.kvstore.protocol.request.SetRequest;
import com.ebay.kvstore.protocol.request.StatRequest;
import com.ebay.kvstore.protocol.response.GetResponse;
import com.ebay.kvstore.protocol.response.IResponse;
import com.ebay.kvstore.protocol.response.IncrResponse;
import com.ebay.kvstore.protocol.response.RegionTableResponse;
import com.ebay.kvstore.protocol.response.StatResponse;
import com.ebay.kvstore.structure.DataServerStruct;

public class SyncKVClient extends BaseKVClient {

	private Logger logger = LoggerFactory.getLogger(SyncKVClient.class);

	public SyncKVClient(ClientOption option) {
		super(option);
		DummyHandler handler = new DummyHandler();
		this.dispatcher.registerHandler(IProtocolType.Get_Resp, handler);
		this.dispatcher.registerHandler(IProtocolType.Set_Resp, handler);
		this.dispatcher.registerHandler(IProtocolType.Delete_Resp, handler);
		this.dispatcher.registerHandler(IProtocolType.Incr_Resp, handler);
		this.dispatcher.registerHandler(IProtocolType.Stat_Resp, handler);
		this.dispatcher.registerHandler(IProtocolType.Region_Table_Resp, handler);
	}

	@Override
	public synchronized void delete(byte[] key) throws KVException {
		checkKey(key);
		delete(key, true);
	}

	@Override
	public synchronized GetResult get(byte[] key) throws KVException {
		checkKey(key);
		return get(key, true);
	}

	@Override
	public IKVClientHandler getClientHandler() {
		throw new UnsupportedOperationException("SyncKVClient does not support client handler");
	}

	@Override
	public synchronized GetResult getCounter(byte[] key) throws KVException {
		GetResult result = get(key);
		return result;
	}

	@Override
	public synchronized int incr(byte[] key, int incremental, int initValue) throws KVException {
		return incr(key, incremental, initValue, 0);
	}

	@Override
	public int incr(byte[] key, int incremental, int initValue, int ttl) throws KVException {
		checkKey(key);
		return incr(key, incremental, initValue, ttl, true);
	}

	@Override
	public synchronized void set(byte[] key, byte[] value) throws KVException {
		set(key, value, 0);
	}

	@Override
	public void set(byte[] key, byte[] value, int ttl) throws KVException {
		checkKey(key);
		if (value == null) {
			throw new NullPointerException("null value is not allowed");
		}
		set(key, value, ttl, true);
	}

	@Override
	public synchronized void setHandler(IKVClientHandler handler) {
		throw new UnsupportedOperationException("Should not set IClientHandler for SyncKVClient");
	}

	@Override
	public synchronized DataServerStruct[] stat() throws KVException {
		return stat(true);
	}

	@Override
	public synchronized void updateRegionTable() throws KVException {
		IoSession session = getMasterConnection();
		IProtocol protocol = new RegionTableRequest();
		try {
			session.write(protocol).await();
			ReadFuture read = session.read();
			read.await();
			RegionTableResponse response = (RegionTableResponse) read.getMessage();
			int ret = response.getRetCode();
			if (ret != ProtocolCode.Success) {
				throw new KVException(ProtocolCode.getMessage(ret));
			}
			this.table = response.getTable();
		} catch (InterruptedException e) {
			logger.error("Wait method has been interrupted", e);
			throw new RuntimeException(e);
		}
	}

	private synchronized void delete(byte[] key, boolean retry) throws KVException {
		IoSession session = getConnection(key);
		IProtocol request = new DeleteRequest(key);
		ReadFuture read = null;
		try {
			session.write(request).await();
			read = session.read();
			read.await();
			IResponse response = (IResponse) read.getMessage();
			int ret = response.getRetCode();
			if (ret == ProtocolCode.Invalid_Key && retry) {
				updateRegionTable();
				delete(key, false);
				return;
			} else if (ret != ProtocolCode.Success) {
				throw new KVException(ProtocolCode.getMessage(ret));
			}
		} catch (InterruptedException e) {
			logger.error("Wait method has been interrupted", e);
			throw new RuntimeException(e);
		}
	}

	private synchronized GetResult get(byte[] key, boolean retry) throws KVException {
		IoSession session = getConnection(key);
		IProtocol request = new GetRequest(key);
		ReadFuture read = null;
		try {
			session.write(request).await();
			read = session.read();
			read.await();
			IResponse response = (IResponse) read.getMessage();
			int ret = response.getRetCode();
			if (ret == ProtocolCode.Invalid_Key && retry) {
				updateRegionTable();
				return get(key, false);
			} else if (ret != ProtocolCode.Success) {
				throw new KVException(ProtocolCode.getMessage(ret));
			} else {
				GetResponse resp = (GetResponse) response;
				return new GetResult(resp.getKey(), resp.getValue(), resp.getTtl());
			}
		} catch (InterruptedException e) {
			logger.error("Wait method has been interrupted", e);
			throw new RuntimeException(e);
		}
	}

	private synchronized int incr(byte[] key, int incremental, int initValue, int ttl, boolean retry)
			throws KVException {
		IoSession session = getConnection(key);
		IProtocol request = new IncrRequest(key, incremental, initValue, ttl);
		ReadFuture read = null;
		try {
			session.write(request).await();
			read = session.read();
			read.await();
			IResponse response = (IResponse) read.getMessage();
			int ret = response.getRetCode();
			if (ret == ProtocolCode.Invalid_Key && retry) {
				updateRegionTable();
				return incr(key, incremental, initValue, ttl, false);
			} else if (ret != ProtocolCode.Success) {
				throw new KVException(ProtocolCode.getMessage(ret));
			} else {
				return ((IncrResponse) response).getValue();
			}
		} catch (InterruptedException e) {
			logger.error("Wait method has been interrupted", e);
			throw new RuntimeException(e);
		}
	}

	private synchronized void set(byte[] key, byte[] value, int ttl, boolean retry)
			throws KVException {
		IoSession session = getConnection(key);
		IProtocol request = new SetRequest(key, value, ttl);
		ReadFuture read = null;
		try {
			session.write(request).await();
			read = session.read();
			read.await();
			IResponse response = (IResponse) read.getMessage();
			int ret = response.getRetCode();
			if (ret == ProtocolCode.Invalid_Key && retry) {
				updateRegionTable();
				set(key, value, ttl, false);
				return;
			} else if (ret != ProtocolCode.Success) {
				throw new KVException(ProtocolCode.getMessage(ret));
			} else {

			}
		} catch (InterruptedException e) {
			logger.error("Wait method has been interrupted", e);
			throw new RuntimeException(e);
		}
	}

	private synchronized DataServerStruct[] stat(boolean retry) throws KVException {
		IoSession session = getMasterConnection();
		IProtocol request = new StatRequest(true);
		ReadFuture read = null;
		try {
			session.write(request).await();
			read = session.read();
			read.await();
			IResponse response = (IResponse) read.getMessage();
			int ret = response.getRetCode();
			if (ret == ProtocolCode.Invalid_Key && retry) {
				updateRegionTable();
				return stat(false);
			} else if (ret != ProtocolCode.Success) {
				throw new KVException(ProtocolCode.getMessage(ret));
			} else {
				return ((StatResponse) (response)).getServers();
			}
		} catch (InterruptedException e) {
			logger.error("Wait method has been interrupted", e);
			throw new RuntimeException(e);
		}
	}

	private class DummyHandler implements IProtocolHandler<IContext, IProtocol> {

		@Override
		public void handle(IContext context, IProtocol protocol) {

		}

	}
}
