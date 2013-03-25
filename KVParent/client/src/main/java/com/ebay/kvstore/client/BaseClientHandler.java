package com.ebay.kvstore.client;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.handler.IProtocolHandler;

public abstract class BaseClientHandler<P extends IProtocol> implements
		IProtocolHandler<ClientContext, P> {

}
