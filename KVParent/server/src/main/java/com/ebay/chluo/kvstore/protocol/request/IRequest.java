package com.ebay.chluo.kvstore.protocol.request;

import java.io.Serializable;

public interface IRequest extends Serializable {
	public int getType();
}
