package com.ebay.kvstore.exception;

public class InvalidKeyException extends KVException {

	private static final long serialVersionUID = 1L;

	public InvalidKeyException(String msg) {
		super(msg);
	}
}