package com.ebay.kvstore.client.async;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.client.IKVClientHandler;
import com.ebay.kvstore.client.IKVClient;
import com.ebay.kvstore.client.async.result.GetResult;
import com.ebay.kvstore.client.async.result.SetResult;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.SetRequest;
import com.ebay.kvstore.protocol.response.SetResponse;

public class AsyncSetResponseHandler extends AsyncClientHandler<SetResponse> {

	@Override
	public void handle(AsyncClientContext context, SetResponse protocol) {
		IKVClient client = context.getClient();
		IoSession session = context.getSession();
		IKVClientHandler handler = client.getClientHandler();
		int ret = protocol.getRetCode();
		boolean retry = protocol.isRetry();
		SetResult result = null;
		try {
			if (ret == ProtocolCode.InvalidKey && retry) {
				client.updateRegionTable();
				session.write(new SetRequest(protocol.getKey(), protocol.getValue(), retry));
				return;
			} else if (ret != ProtocolCode.Success) {
				result = new SetResult(protocol.getKey(), protocol.getValue(), new KVException(
						ProtocolCode.getMessage(ret)));
			} else {
				result = new SetResult(protocol.getKey(), protocol.getValue(), null);
			}
		} catch (KVException e) {
			result = new SetResult(protocol.getKey(), protocol.getValue(), e);
		}
		handler.onSet(result);
	}

}
