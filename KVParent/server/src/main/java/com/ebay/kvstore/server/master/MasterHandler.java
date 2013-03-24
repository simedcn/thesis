package com.ebay.kvstore.server.master;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.handler.IProtocolHandler;

/**
 * 
 * @author luochen
 * 
 */
public abstract class MasterHandler<P extends IProtocol> implements
		IProtocolHandler<MasterContext, P> {

}
