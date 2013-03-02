package com.ebay.chluo.kvstore.protocol;

import java.io.Serializable;

public class SimpleResponse implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String message;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public SimpleResponse(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "SimpleResponse [message=" + message + "]";
	}

}
