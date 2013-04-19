package com.ebay.kvstore.client.async;

import org.apache.mina.core.session.IoSession;

import com.ebay.kvstore.client.IKVClientHandler;
import com.ebay.kvstore.client.IKVClient;
import com.ebay.kvstore.client.result.DeleteResult;
import com.ebay.kvstore.exception.KVException;
import com.ebay.kvstore.protocol.ProtocolCode;
import com.ebay.kvstore.protocol.request.DeleteRequest;
import com.ebay.kvstore.protocol.response.DeleteResponse;

public class AsyncDeleteResponseHandler extends AsyncClientHandler<DeleteResponse> {

	@Override
	public void handle(AsyncClientContext context, DeleteResponse protocol) {
		IKVClient client = context.getClient();
		IoSession session = context.getSession();
		IKVClientHandler handler = client.getClientHandler();
		boolean retry = protocol.isRetry();
		int ret = protocol.getRetCode();
		DeleteResult result = null;
		try {
			if (ret == ProtocolCode.Invalid_Key && retry) {
				client.updateRegionTable();
				session.write(new DeleteRequest(protocol.getKey(), false));
				return;
			} else if (ret != ProtocolCode.Success) {
				result = new DeleteResult(protocol.getKey(), new KVException(
						ProtocolCode.getMessage(ret)));
			} else {
				result = new DeleteResult(protocol.getKey());
			}
		} catch (KVException e) {
			result = new DeleteResult(protocol.getKey(), e);
		}
		handler.onDelete(result);
	}

}
