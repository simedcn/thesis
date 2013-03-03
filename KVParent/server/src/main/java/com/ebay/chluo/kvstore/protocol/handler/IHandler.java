package com.ebay.chluo.kvstore.protocol.handler;

import com.ebay.chluo.kvstore.protocol.request.IRequest;
import com.ebay.chluo.kvstore.protocol.response.IResponse;

public interface IHandler {

	public void handleRequest(IContext context, IRequest request);

	public void handlerResponse(IContext context, IResponse response);
}
