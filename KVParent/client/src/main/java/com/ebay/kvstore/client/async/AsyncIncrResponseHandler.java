package com.ebay.kvstore.client.async;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.client.IKVClientHandler;
import com.ebay.kvstore.client.IKVClient;
import com.ebay.kvstore.client.result.IncrResult;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.IncrRequest;
import com.ebay.kvstore.protocol.response.IncrResponse;

public class AsyncIncrResponseHandler extends AsyncClientHandler<IncrResponse> {

	@Override
	public void handle(AsyncClientContext context, IncrResponse protocol) {
		IKVClient client = context.getClient();
		IoSession session = context.getSession();
		IKVClientHandler handler = client.getClientHandler();
		int ret = protocol.getRetCode();
		boolean retry = protocol.isRetry();
		IncrResult result = null;
		try {
			if (ret == ProtocolCode.Invalid_Key && retry) {
				client.updateRegionTable();
				session.write(new IncrRequest(protocol.getKey(), protocol.getIncremental(),
						protocol.getValue(), protocol.getTtl(), false));
				return;
			} else if (ret != ProtocolCode.Success) {
				result = new IncrResult(protocol.getKey(), protocol.getValue(), new KVException(
						ProtocolCode.getMessage(ret)));
			} else {
				result = new IncrResult(protocol.getKey(), protocol.getValue());
			}
		} catch (KVException e) {
			result = new IncrResult(protocol.getKey(), protocol.getValue(), e);
		}
		handler.onIncr(result);
	}

}
