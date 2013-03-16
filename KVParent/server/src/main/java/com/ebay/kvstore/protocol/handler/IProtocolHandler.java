package com.ebay.kvstore.protocol.handler;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.context.IContext;

public interface IProtocolHandler<C extends IContext, P extends IProtocol> {

	public void handle(C context, P protocol);
}
