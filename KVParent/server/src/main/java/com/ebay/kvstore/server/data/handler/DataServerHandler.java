package com.ebay.kvstore.server.data.handler;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.handler.IProtocolHandler;
import com.ebay.kvstore.server.data.DataServerContext;

/**
 * TODO:Currently, the response of data server will contains the raw key/value,
 * which may consume the bandwidth.
 * 
 * @author luochen
 * 
 * @param <P>
 */
public abstract class DataServerHandler<P extends IProtocol> implements
		IProtocolHandler<DataServerContext, P> {

}
