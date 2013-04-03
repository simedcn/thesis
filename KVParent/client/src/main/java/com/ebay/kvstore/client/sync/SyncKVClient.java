package com.ebay.kvstore.client.sync;

import java.util.Arrays;

import org.apache.mina.core.future.ReadFuture;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.client.BaseClient;
import com.ebay.kvstore.client.ClientOption;
import com.ebay.kvstore.client.IKVClientHandler;
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

public class SyncKVClient extends BaseClient {

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
			if (ret == ProtocolCode.InvalidKey && retry) {
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

	public synchronized void delete(byte[] key) throws KVException {
		delete(key, true);
	}

	private synchronized byte[] get(byte[] key, boolean retry) throws KVException {
		IoSession session = getConnection(key);
		IProtocol request = new GetRequest(key);
		ReadFuture read = null;
		try {
			session.write(request).await();
			read = session.read();
			read.await();
			IResponse response = (IResponse) read.getMessage();
			int ret = response.getRetCode();
			if (ret == ProtocolCode.InvalidKey && retry) {
				updateRegionTable();
				return get(key, false);
			} else if (ret != ProtocolCode.Success) {
				throw new KVException(ProtocolCode.getMessage(ret));
			} else {
				return ((GetResponse) response).getValue();
			}
		} catch (InterruptedException e) {
			logger.error("Wait method has been interrupted", e);
			throw new RuntimeException(e);
		}
	}

	public synchronized byte[] get(byte[] key) throws KVException {
		return get(key, true);
	}

	public IKVClientHandler getClientHandler() {
		throw new UnsupportedOperationException("Should not get IClientHandler for SyncKVClient");
	}

	public synchronized int getCounter(byte[] key) throws KVException {
		byte[] value = get(key);
		if (value == null || value.length != 4) {
			throw new KVException("The key:" + Arrays.toString(key) + " is not a valid counter");
		}
		return KeyValueUtil.bytesToInt(value);
	}

	private synchronized int incr(byte[] key, int incremental, int initValue, boolean retry)
			throws KVException {
		IoSession session = getConnection(key);
		IProtocol request = new IncrRequest(key, incremental, initValue);
		ReadFuture read = null;
		try {
			session.write(request).await();
			read = session.read();
			read.await();
			IResponse response = (IResponse) read.getMessage();
			int ret = response.getRetCode();
			if (ret == ProtocolCode.InvalidKey && retry) {
				updateRegionTable();
				return incr(key, incremental, initValue, false);
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

	public synchronized int incr(byte[] key, int incremental, int initValue) throws KVException {
		return incr(key, incremental, initValue, true);
	}

	public synchronized void setHandler(IKVClientHandler handler) {
		throw new UnsupportedOperationException("Should not set IClientHandler for SyncKVClient");
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
			if (ret == ProtocolCode.InvalidKey && retry) {
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

	@Override
	public synchronized void set(byte[] key, byte[] value) throws KVException {
		set(key, value, true);
	}

	private synchronized void set(byte[] key, byte[] value, boolean retry) throws KVException {
		IoSession session = getConnection(key);
		IProtocol request = new SetRequest(key, value);
		ReadFuture read = null;
		try {
			session.write(request).await();
			read = session.read();
			read.await();
			IResponse response = (IResponse) read.getMessage();
			int ret = response.getRetCode();
			if (ret == ProtocolCode.InvalidKey && retry) {
				updateRegionTable();
				set(key, value, false);
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

	// TODO stat
	public synchronized DataServerStruct[] stat() throws KVException {
		return stat(true);
	}

	private class DummyHandler implements IProtocolHandler<IContext, IProtocol> {

		@Override
		public void handle(IContext context, IProtocol protocol) {

		}

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
}
