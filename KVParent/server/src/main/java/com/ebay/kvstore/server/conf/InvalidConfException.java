package com.ebay.kvstore.server.conf;

public class InvalidConfException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String msg;

	public InvalidConfException(String confKey, String expectedValue, String realValue) {
		super();
		StringBuilder sb = new StringBuilder();
		sb.append("Invalid configuration:");
		sb.append(confKey);
		sb.append("   expectedValue:");
		sb.append(expectedValue);
		sb.append("   realValue:");
		sb.append(realValue);
		msg = sb.toString();
	}

	@Override
	public String getMessage() {
		return msg;
	}
}
