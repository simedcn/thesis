package com.ebay.kvstore.server.data;

import com.ebay.kvstore.protocol.IProtocol;
import com.ebay.kvstore.protocol.handler.IProtocolHandler;

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
