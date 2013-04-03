package com.ebay.kvstore.server.master.handler;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.handler.IProtocolHandler;
import com.ebay.kvstore.server.master.MasterContext;

/**
 * 
 * @author luochen
 * 
 */
public abstract class MasterHandler<P extends IProtocol> implements
		IProtocolHandler<MasterContext, P> {

}
