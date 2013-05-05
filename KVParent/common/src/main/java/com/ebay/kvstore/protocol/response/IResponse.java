package com.ebay.kvstore.protocol.response;

import com.ebay.kvstore.protocol.IProtocol;

public interface IResponse extends IProtocol {

	public int getRetCode();
}
