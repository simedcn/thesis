package com.ebay.kvstore.protocol.request;

public abstract class ClientRequest implements IRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected boolean retry;

	public ClientRequest() {
		retry = true;
	}

	public ClientRequest(boolean retry) {
		this.retry = retry;
	}

	public boolean isRetry() {
		return retry;
	}

	public void setRetry(boolean retry) {
		this.retry = retry;
	}
	// do nothing
}
