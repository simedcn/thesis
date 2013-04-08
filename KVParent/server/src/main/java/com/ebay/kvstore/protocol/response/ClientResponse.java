package com.ebay.kvstore.protocol.response;

public abstract class ClientResponse extends BaseResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected boolean retry;

	public ClientResponse(int retCode) {
		super(retCode);
		this.retry = true;
	}

	public ClientResponse(int retCode, boolean retry) {
		super(retCode);
		this.retry = retry;
	}

	public void setRetry(boolean retry) {
		this.retry = retry;
	}

	public boolean isRetry() {
		return retry;
	}
}
