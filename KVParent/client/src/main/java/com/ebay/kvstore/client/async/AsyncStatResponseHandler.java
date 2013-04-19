package com.ebay.kvstore.client.async;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.client.IKVClientHandler;
import com.ebay.kvstore.client.IKVClient;
import com.ebay.kvstore.client.result.StatResult;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.StatRequest;
import com.ebay.kvstore.protocol.response.StatResponse;

public class AsyncStatResponseHandler extends AsyncClientHandler<StatResponse> {

	@Override
	public void handle(AsyncClientContext context, StatResponse protocol) {
		IKVClient client = context.getClient();
		IoSession session = context.getSession();
		IKVClientHandler handler = client.getClientHandler();
		int ret = protocol.getRetCode();
		boolean retry = protocol.isRetry();
		StatResult result = null;
		try {
			if (ret == ProtocolCode.Invalid_Key && retry) {
				client.updateRegionTable();
				session.write(new StatRequest(false));
				return;
			} else if (ret != ProtocolCode.Success) {
				result = new StatResult(null, new KVException(ProtocolCode.getMessage(ret)));
			} else {
				result = new StatResult(protocol.getServers(), null);
			}
		} catch (KVException e) {
			result = new StatResult(null, e);
		}
		handler.onStat(result);
	}
}
