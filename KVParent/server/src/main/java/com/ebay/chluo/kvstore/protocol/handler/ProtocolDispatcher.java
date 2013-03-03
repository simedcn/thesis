package com.ebay.chluo.kvstore.protocol.handler;

import java.util.HashMap;
import java.util.Map;

import com.ebay.chluo.kvstore.protocol.IProtocol;
import com.ebay.chluo.kvstore.protocol.context.IContext;

public class ProtocolDispatcher {

	private Map<Integer, IProtocolHandler> handlers;

	public ProtocolDispatcher() {
		handlers = new HashMap<>();
	}

	public void registerHandler(int type, IProtocolHandler handler) {
		handlers.put(type, handler);
	}

	public IProtocolHandler unregisterHandler(int type) {
		return handlers.remove(type);
	}

	public void handle(Object obj, IContext context) {
		if (!(obj instanceof IProtocol)) {
			throw new IllegalArgumentException(
					"The message object is not a valid protocol message:" + obj);
		}
		IProtocol protocol = (IProtocol) obj;
		IProtocolHandler handler = handlers.get(protocol.getType());
		if (handler == null) {
			throw new NullPointerException("The IProtocolHandler for ProtocolType:"
					+ protocol.getType() + " has not been registered");
		}
		handler.handle(context, protocol);
	}
}
