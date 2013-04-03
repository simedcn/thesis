package com.ebay.kvstore.client.async;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.client.IKVClientHandler;
import com.ebay.kvstore.client.IKVClient;
import com.ebay.kvstore.client.async.result.GetResult;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.GetRequest;
import com.ebay.kvstore.protocol.response.GetResponse;

public class AsyncGetResponseHandler extends AsyncClientHandler<GetResponse> {

	@Override
	public void handle(AsyncClientContext context, GetResponse protocol) {
		IKVClient client = context.getClient();
		IoSession session = context.getSession();
		IKVClientHandler handler = client.getClientHandler();
		int ret = protocol.getRetCode();
		boolean retry = protocol.isRetry();
		GetResult result = null;
		try {
			if (ret == ProtocolCode.InvalidKey && retry) {
				client.updateRegionTable();
				session.write(new GetRequest(protocol.getKey(), retry));
				return;
			} else if (ret != ProtocolCode.Success) {
				result = new GetResult(protocol.getKey(), null, new KVException(
						ProtocolCode.getMessage(ret)));
			} else {
				result = new GetResult(protocol.getKey(), protocol.getValue());
			}
		} catch (KVException e) {
			result = new GetResult(protocol.getKey(), null, e);
		}
		handler.onGet(result);
	}
}
