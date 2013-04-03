package com.ebay.kvstore.client.async;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.handler.IProtocolHandler;

public abstract class AsyncClientHandler<P extends IProtocol> implements
		IProtocolHandler<AsyncClientContext, P> {

}
