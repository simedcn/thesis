package com.ebay.kvstore.server.data.handler;

import java.io.IOException;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.KeyValueUtil;
import com.ebay.kvstore.exception.InvalidKeyException;
import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.IncrRequest;
import com.ebay.kvstore.protocol.response.IncrResponse;
import com.ebay.kvstore.server.data.DataServerContext;
import com.ebay.kvstore.server.data.storage.IStoreEngine;
import com.ebay.kvstore.structure.KeyValue;

public class IncrRequestHandler extends DataServerHandler<IncrRequest> {

	@Override
	public void handle(DataServerContext context, IncrRequest protocol) {
		IStoreEngine engine = context.getEngine();
		IoSession session = context.getSession();
		IProtocol response = null;
		int incremental = protocol.getIncremental();
		int initValue = protocol.getInitValue();
		byte[] key = protocol.getKey();
		boolean retry = protocol.isRetry();
		try {
			KeyValue kv = engine.incr(key, incremental, initValue);
			byte[] value = KeyValueUtil.getValue(kv);
			response = new IncrResponse(ProtocolCode.Success, key, incremental,
					KeyValueUtil.bytesToInt(value), retry);
		} catch(UnsupportedOperationException e){
			response = new IncrResponse(ProtocolCode.Invalid_Counter, key, incremental, initValue, retry);
		}
		catch (InvalidKeyException e) {
			response = new IncrResponse(ProtocolCode.Invalid_Key, key, incremental, initValue, retry);
		} catch (IOException e) {
			response = new IncrResponse(ProtocolCode.Dataserver_Io_Error, key, incremental, initValue, retry);
		}
		session.write(response);
	}

}
