package com.ebay.chluo.kvstore.protocol.handler;

import com.ebay.chluo.kvstore.protocol.IProtocol;
import com.ebay.chluo.kvstore.protocol.context.IContext;

public interface IProtocolHandler< C extends IContext, P extends IProtocol>{

	public void handle(C context, P protocol);
}
